package fp.jadobec;

import static org.junit.Assert.*;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.function.Function;

import fp.jadobec.DbCommand;
import fp.jadobec.Record;
import fp.jadobec.Repository;

import fp.util.Either;
import fp.util.Failure;
import fp.util.Left;
import fp.util.Right;
import fp.util.StreamUtil;
import fp.util.Tuple2;

public class ContactTest {
    @Test
    public void testTempUsers() {
        final List<Either<Failure, User>> expectedUsers = Arrays.asList(
            User.of(3, "Jake Doe"),
            User.of(2, "Jane Doe"),
            User.of(1, "John Doe")
        );

        checkDbCommand(
            createAndFill.then(
                queryUsers()
            ).forEach(users ->
                assertArrayEquals(
                    expectedUsers.toArray(),
                    users.collect(Collectors.toList()).toArray()
                )
            )
        );
    }

    @Test
    public void testSingleContact() {
        final List<Either<Failure, User>> expectedUsers = Arrays.asList(
            Left.of(Failure.of("Missing result")),
            User.of(2, "Jane Doe").map(user ->
                user.addEmail(Email.of("jane@doe.com", false))
            ),
            User.of(1, "John Doe").map(user ->
                user.addEmail(Email.of("john@doe.com", true))
            )
        );

        checkDbCommand(
            createAndFill.then(
                queryUsers()
                    .mapListEither(ContactTest::addOneEmail)
            ).forEach(users ->
                assertArrayEquals(expectedUsers.toArray(), users.toArray())
            )
        );
    }

    @Test
    public void testFailedSingleContact() {
        checkDbCommand(
            createAndFill.then(
                queryUsers()
                    .mapListEither(ContactTest::addOneEmail)
                    .map(items -> items.noneMatch(Either::isRight))
            ).forEach(isFailure -> assertFalse(isFailure))
        );
    }

    @Test
    public void testContacts() {
        final List<Either<Failure, User>> expectedUsers = Arrays.asList(
            User.of(3, "Jake Doe"),
            User.of(2, "Jane Doe").map(user ->
                user.addEmail(Email.of("jane.doe@doe.com", true))
            ),
            User.of(1, "John Doe").map(user ->
                user.addEmail(Email.of("john@doe.com", true))
            )
        );

        checkDbCommand(
            createAndFill.then(
                queryUsers()
                    .mapListEither(ContactTest::addEmails)
            ).forEach(users ->
                assertArrayEquals(expectedUsers.toArray(), users.toArray())
            )
        );
    }

    @Test
    public void testPartialLoad() {
        final Either<Failure, User> expectedUser = User.of(2, "Jane Doe");
        final DbCommand<Either<Failure, User>> dbCommandIdCheckedUser =
            createAndFill.then(
                queryUserIds()
                    .map(items -> items
                        .filter(ContactTest::checkUserIdSlow)
                        .findFirst()
                    )
                    .flatMap(ContactTest::querySingleUser)
            );

        checkDbCommand(
            dbCommandIdCheckedUser
                .forEach(user ->
                    assertEquals(expectedUser, user)
                )
        );
    }

    private static final DbCommand<Integer> createAndFill =
        Repository.transaction(() ->
            createDb().then(
                insertData()
            )
        );

    private static Either<Failure, Repository> loadRepository() {
        return Repository.load(
            "org.h2.jdbcx.JdbcDataSource",
            "SELECT 1",
            Tuple2.of("URL", "jdbc:h2:mem:")
        );
    }

    private static DbCommand<Integer> createDb() {
        return Repository.batchUpdate(
            "CREATE TABLE user(" +
                "id_user INT auto_increment, " +
                "name VARCHAR(50) NOT NULL " +
            ")",
            "CREATE INDEX user_name ON user(name)",
            "CREATE TABLE email(" +
                "id_email INT auto_increment, " +
                "id_user INT NOT NULL, " +
                "email VARCHAR(50) NOT NULL, " +
                "importance INT NULL, " +
                "validated BOOLEAN NOT NULL " +
            ")",
            "CREATE INDEX email_email ON email(email)"
        );
    }

    private static DbCommand<Integer> insertData() {
        return Repository.batchUpdate(
            "INSERT INTO user(id_user, name) VALUES(1, 'John Doe')",
            "INSERT INTO email(id_user, email, validated) " +
            "  VALUES(1, 'john.doe@doe.com', '0')",
            "INSERT INTO email(id_user, email, validated) " +
            "  VALUES(1, 'john@doe.com', '1')",
            "INSERT INTO user(id_user, name) VALUES(2, 'Jane Doe')",
            "INSERT INTO email(id_user, email, validated) " +
            "  VALUES(2, 'jane.doe@doe.com', '1')",
            "INSERT INTO email(id_user, email, validated, importance) " +
            "  VALUES(2, 'jane@doe.com', '0', 1)",
            "INSERT INTO email(id_user, email, validated) " +
            "  VALUES(2, 'janedoe@doe.com', '1')",
            "INSERT INTO user(id_user, name) VALUES(3, 'Jake Doe')"
        );
    }

    private static DbCommand<Stream<Either<Failure, User>>> queryUsers() {
        return Repository.query(
            "SELECT id_user, name FROM user ORDER BY name",
            rs -> User.of(rs.getInt(1), rs.getString(2))
        );
    }

    private static DbCommand<User> addOneEmail(final User user) {
        return querySingleEmail(user)
            .map(user::addEmail)
        ;
    }

    private static DbCommand<User> addEmails(final User user) {
        return queryEmails(user)
            .map(StreamUtil.reduce(user, User::addEmail))
        ;
    }

    private static DbCommand<Email> querySingleEmail(User user) {
        return Repository.querySingle(
            "SELECT email, validated " +
                "FROM email " +
                "WHERE id_user=? " +
                "ORDER BY importance desc, validated desc",
            rs -> Email.of(rs.getString(1), rs.getBoolean(2)),
            user.getId()
        );
    }

    private static DbCommand<Stream<Email>> queryEmails(User user) {
        return Repository.query(
            "SELECT email, validated " +
                "FROM email " +
                "WHERE id_user=? " +
                "ORDER BY importance desc, validated desc",
            rs -> Email.of(rs.getString(1), rs.getBoolean(2)),
            user.getId()
        );
    }

    private static DbCommand<Stream<Integer>> queryUserIds() {
        return Repository.query(
            "SELECT id_user FROM user ORDER BY name",
            rs -> rs.getInt(1)
        );
    }

    private static boolean checkUserIdSlow(Integer id) {
        return (id == 2);
    }

    private static DbCommand<Either<Failure, User>>
        querySingleUser(Optional<Integer> idOpt)
    {
        if (idOpt.isPresent()) {
            return Repository.querySingle(
                "SELECT id_user, name FROM user where id_user = ?",
                rs -> User.of(rs.getInt(1), rs.getString(2)),
                idOpt.get()
            );
        } else {
            return connection -> Left.of(Failure.of("Missing user id"));
        }
    }

    private static <T> void checkDbCommand(DbCommand<T> testDbCommand) {
        final Either<Failure, T> repositoryOrFailure = loadRepository()
            .flatMap(repository ->
                repository.use(
                    testDbCommand
                )
            );

        assertTrue(
            repositoryOrFailure.toString(),
            repositoryOrFailure.right().isPresent()
        );
    }

    private static interface User {
        public static Either<Failure, User> of(int id, String name) {
            if (name.trim().isEmpty()) {
                return Left.of(
                    Failure.of("Wrong user name")
                );
            } else {
                return Right.of(
                    new TempUser(id, name, Optional.empty())
                );
            }
        }
        public User addEmail(Email email);
        public int getId();
    }

    private static class TempUser implements User {
        private final int id;
        private final String name;
        private final Optional<TempEmail> email;

        private TempUser(int id, String name, Optional<TempEmail> email) {
            this.id = id;
            this.name = name;
            this.email = email;
        }

        public User addEmail(Email email) {
            if (email instanceof TempEmail) {
                return new TempUser(id, name, Optional.of((TempEmail) email));
            } else if (email instanceof ValidatedEmail) {
                return new ValidatedUser(id, name, (ValidatedEmail) email);
            } else {
                return this;
            }
        }

        public int getId() {
            return id;
        }

        @Override
        public String toString() {
            return "TempUser(" + id + ", " + name + ", " + email + ")";
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof TempUser) {
                TempUser user = (TempUser) other;
                return Objects.equals(id, user.id)
                    && Objects.equals(name, user.name)
                    && Objects.equals(email, user.email);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, email);
        }
    }

    private static class ValidatedUser implements User {
        private final int id;
        private final String name;
        private final ValidatedEmail email;

        private ValidatedUser(int id, String name, ValidatedEmail email) {
            this.id = id;
            this.name = name;
            this.email = email;
        }

        public User addEmail(Email email) {
            return this;
        }

        public int getId() {
            return id;
        }

        @Override
        public String toString() {
            return "ValidatedUser(" + id + ", " + name + ", " + email + ")";
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof ValidatedUser) {
                ValidatedUser user = (ValidatedUser) other;
                return Objects.equals(id, user.id)
                    && Objects.equals(name, user.name)
                    && Objects.equals(email, user.email);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, email);
        }
    }

    private static interface Email {
        public static Email of(String email, boolean validated) {
            if (validated) {
                return new ValidatedEmail(email);
            } else {
                return new TempEmail(email);
            }
        }
    }

    private static class TempEmail implements Email {
        private final String email;

        private TempEmail(String email) {
            this.email = email;
        }

        @Override
        public String toString() {
            return "TempEmail(" + email + ")";
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof TempEmail) {
                TempEmail tempEmail = (TempEmail) other;
                return Objects.equals(email, tempEmail.email);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(email);
        }
    }

    private static class ValidatedEmail implements Email {
        private final String email;

        private ValidatedEmail(String email) {
            this.email = email;
        }

        @Override
        public String toString() {
            return "ValidatedEmail(" + email + ")";
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof ValidatedEmail) {
                ValidatedEmail validatedEmail = (ValidatedEmail) other;
                return Objects.equals(email, validatedEmail.email);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(email);
        }
    }
}
