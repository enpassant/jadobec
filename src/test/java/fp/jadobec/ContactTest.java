package fp.jadobec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.Test;

import fp.io.DefaultPlatform;
import fp.io.DefaultRuntime;
import fp.io.Environment;
import fp.io.IO;
import fp.io.Runtime;
import fp.util.Either;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Right;
import fp.util.StreamUtil;
import fp.util.Tuple2;

public class ContactTest {
    final static DefaultPlatform platform = new DefaultPlatform();

    final static Runtime<Void> defaultRuntime = new DefaultRuntime<Void>(null, platform);

    @AfterClass
    public static void setUp() {
        platform.shutdown();
    }

    @Test
    public void testTempUsers() {
        final List<Either<Failure, User>> expectedUsers = Arrays.asList(
            User.of(3, "Jake Doe"),
            User.of(2, "Jane Doe"),
            User.of(1, "John Doe")
        );

        checkDbCommand(connection ->
            createAndFill(connection).flatMap(v ->
                queryUsers(connection)
            ).peek(users ->
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
            Left.of(GeneralFailure.of("Missing result")),
            User.of(2, "Jane Doe").map(user ->
                user.addEmail(Email.of("jane@doe.com", false))
            ),
            User.of(1, "John Doe").map(user ->
                user.addEmail(Email.of("john@doe.com", true))
            )
        );

        checkDbCommand(connection ->
            createAndFill(connection).flatMap(v ->
                mapStreamEither(
                    connection,
                    queryUsers(connection),
                    user -> ContactTest.addOneEmail(connection, user)
                )
            ).peek(users ->
                assertArrayEquals(expectedUsers.toArray(), users.toArray())
            )
        );
    }

    @Test
    public void testFailedSingleContact() {
        checkDbCommand(connection ->
            createAndFill(connection).flatMap(v ->
                mapStreamEither(
                    connection,
                    queryUsers(connection),
                    user -> ContactTest.addOneEmail(connection, user)
                ).map(items -> items.noneMatch(Either::isRight))
            ).peek(isFailure -> assertFalse(isFailure))
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

        checkDbCommand(connection ->
            createAndFill(connection).flatMap(v ->
                mapStreamEither(
                    connection,
                    queryUsers(connection),
                    user -> ContactTest.addEmails(connection, user)
                )
            ).peek(users ->
                assertArrayEquals(expectedUsers.toArray(), users.toArray())
            )
        );
    }

    private static <F, R, T, U> IO<Environment, F, Stream<Either<F, R>>> mapStreamEither(
        Connection connection,
        IO<Environment, F, Stream<Either<F, U>>> io,
        Function<U, IO<Environment, F, R>> mapper
    ) {
        return IO.absolve(IO.access(env ->
            defaultRuntime.unsafeRun(io.provide(env))
                .map((Stream<Either<F, U>> items) -> items.map(
                    (Either<F, U> item) -> item.flatMap(
                        v -> defaultRuntime.unsafeRun(
                            mapper.apply(v).provide(env)
                        )
                    )
                )
            ))
        );
    }

    @Test
    public void testPartialLoad() {
        final Either<Failure, User> expectedUser = User.of(2, "Jane Doe");
        final Function<Connection, IO<Environment, Failure, Either<Failure, User>>>
            dbCommandIdCheckedUser = connection ->
            createAndFill(connection).flatMap(v ->
                queryUserIds(connection)
                    .map(items -> items
                        .filter(ContactTest::checkUserIdSlow)
                        .findFirst()
                    )
                    .flatMap(idOpt -> ContactTest.querySingleUser(connection, idOpt))
            );

        checkDbCommand(connection ->
            dbCommandIdCheckedUser.apply(connection)
                .peek(user ->
                    assertEquals(expectedUser, user)
                )
        );
    }

    private static final IO<Environment, Failure, Integer> createAndFill(
        Connection connection
    ) {
        return Repository.transaction(
            connection,
            createDb(connection).flatMap(v ->
                insertData(connection)
            )
        );
    }

    private static Either<Failure, Repository.Live> createRepository() {
        return Repository.Live.create(
            "org.h2.jdbcx.JdbcDataSource",
            "SELECT 1",
            Tuple2.of("URL", "jdbc:h2:mem:")
        );
    }

    private static IO<Environment, Failure, Integer> createDb(Connection connection) {
        return Repository.batchUpdate(
            connection,
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

    private static IO<Environment, Failure, Integer> insertData(Connection connection) {
        return Repository.batchUpdate(
            connection,
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

    private static IO<Environment, Failure, Stream<Either<Failure, User>>>
        queryUsers(Connection connection)
    {
        return Repository.query(
            connection,
            "SELECT id_user, name FROM user ORDER BY name",
            rs -> User.of(rs.getInt(1), rs.getString(2)),
            Repository::iterateToStream
        );
    }

    private static IO<Environment, Failure, User> addOneEmail(
        Connection connection,
        final User user
    ) {
        return querySingleEmail(connection, user)
            .map(user::addEmail)
        ;
    }

    private static IO<Environment, Failure, User> addEmails(
        Connection connection,
        final User user
    ) {
        return queryEmails(connection, user)
            .map(StreamUtil.reduce(user, User::addEmail))
        ;
    }

    private static IO<Environment, Failure, Email> querySingleEmail(
        Connection connection,
        User user
    ) {
        return Repository.querySingle(
            connection,
            "SELECT email, validated " +
                "FROM email " +
                "WHERE id_user=? " +
                "ORDER BY importance desc, validated desc",
            rs -> Email.of(rs.getString(1), rs.getBoolean(2)),
            user.getId()
        );
    }

    private static IO<Environment, Failure, Stream<Email>> queryEmails(
        Connection connection, User user
    ) {
        return Repository.query(
            connection,
            "SELECT email, validated " +
                "FROM email " +
                "WHERE id_user=? " +
                "ORDER BY importance desc, validated desc",
            rs -> Email.of(rs.getString(1), rs.getBoolean(2)),
            Repository::iterateToStream,
            user.getId()
        );
    }

    private static IO<Environment, Failure, Stream<Integer>> queryUserIds(
        Connection connection
    ) {
        return Repository.query(
            connection,
            "SELECT id_user FROM user ORDER BY name",
            rs -> rs.getInt(1),
            Repository::iterateToStream
        );
    }

    private static boolean checkUserIdSlow(Integer id) {
        return (id == 2);
    }

    private static IO<Environment, Failure, Either<Failure, User>>
        querySingleUser(Connection connection, Optional<Integer> idOpt)
    {
        if (idOpt.isPresent()) {
            return Repository.querySingle(
                connection,
                "SELECT id_user, name FROM user where id_user = ?",
                rs -> User.of(rs.getInt(1), rs.getString(2)),
                idOpt.get()
            );
        } else {
            return IO.fail((Failure) GeneralFailure.of("Missing user id"));
        }
    }

    private static <T> void checkDbCommand(
        Function<Connection, IO<Environment, Failure, T>> testDbCommand
    ) {
        final Either<Failure, T> repositoryOrFailure = createRepository()
            .flatMap(repository -> {
                final Environment environment =
                    Environment.of(Repository.Service.class, repository);
                return defaultRuntime.unsafeRun(
                    IO.bracket(
                        Repository.getConnection(),
                        connection -> IO.effect(() -> connection.close()),
                        connection -> testDbCommand.apply(connection)
                ).provide(environment));
            });

        assertTrue(
            repositoryOrFailure.toString(),
            repositoryOrFailure.isRight()
        );
    }

    private static interface User {
        public static Either<Failure, User> of(int id, String name) {
            if (name.trim().isEmpty()) {
                return Left.of(
                    GeneralFailure.of("Wrong user name")
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
