package fp.jadobec;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import fp.io.Cause;
import fp.io.DefaultPlatform;
import fp.io.DefaultRuntime;
import fp.io.IO;
import fp.io.Runtime;
import fp.util.Either;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Right;
import fp.util.StreamUtil;
import fp.util.Tuple2;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ContactTest
{
    final static DefaultPlatform platform = new DefaultPlatform();

    final static Runtime defaultRuntime =
        new DefaultRuntime(null, platform);

    private static final Repository repoBase = Repository.of();

    @AfterClass
    public static void setUp()
    {
        platform.shutdown();
    }

    @Test
    public void testTempUsers()
    {
        final List<Either<Failure, User>> expectedUsers = Arrays.asList(
            User.of(3, "Jake Doe"),
            User.of(2, "Jane Doe"),
            User.of(1, "John Doe")
        );

        checkDbCommand(
            createAndFill.flatMap(v ->
                queryUsers()
            ).peek(users ->
                assertArrayEquals(
                    expectedUsers.toArray(),
                    users.toList().toArray()
                )
            )
        );
    }

    @Test
    public void testSingleContact()
    {
        final List<Either<Failure, User>> expectedUsers = Arrays.asList(
            Left.of(GeneralFailure.of("Missing result")),
            User.of(2, "Jane Doe").map(user ->
                user.addEmail(Email.of("jane@doe.com", false))
            ),
            User.of(1, "John Doe").map(user ->
                user.addEmail(Email.of("john@doe.com", true))
            )
        );

        checkDbCommand(
            createAndFill.flatMap(v ->
                repoBase.mapStreamEither(
                    queryUsers(),
                    ContactTest::addOneEmail
                )
            ).peek(users ->
                assertArrayEquals(expectedUsers.toArray(), users.toArray())
            )
        );
    }

    @Test
    public void testFailedSingleContact()
    {
        checkDbCommand(
            createAndFill.flatMap(v ->
                repoBase.mapStreamEither(
                    queryUsers(),
                    ContactTest::addOneEmail
                ).map(items -> items.noneMatch(Either::isRight))
            ).peek(Assert::assertFalse)
        );
    }

    @Test
    public void testContacts()
    {
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
            createAndFill.flatMap(v ->
                repoBase.mapStreamEither(
                    queryUsers(),
                    ContactTest::addEmails
                )
            ).peek(users ->
                assertArrayEquals(expectedUsers.toArray(), users.toArray())
            )
        );
    }

    @Test
    public void testPartialLoad()
    {
        final Either<Failure, User> expectedUser = User.of(2, "Jane Doe");
        final IO<Failure, Either<Failure, User>>
            dbCommandIdCheckedUser = createAndFill.flatMap(v ->
            queryUserIds()
                .map(items -> items
                    .filter(ContactTest::checkUserIdSlow)
                    .findFirst()
                )
                .flatMap(ContactTest::querySingleUser)
        );

        checkDbCommand(
            dbCommandIdCheckedUser
                .peek(user ->
                    assertEquals(expectedUser, user)
                )
        );
    }

    private static final IO<Failure, Integer> createAndFill =
        repoBase.transaction(
            createDb().flatMap(v ->
                insertData()
            ));

    private static Either<Failure, Repository.Live> createRepository()
    {
        return Repository.Live.create(
            "org.h2.jdbcx.JdbcDataSource",
            "SELECT 1",
            Tuple2.of("URL", "jdbc:h2:mem:;MODE=MySQL;NON_KEYWORDS=USER")
        );
    }

    private static IO<Failure, Integer> createDb()
    {
        return repoBase.batchUpdate(
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

    private static IO<Failure, Integer> insertData()
    {
        return repoBase.batchUpdate(
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

    private static IO<Failure, Stream<Either<Failure, User>>> queryUsers()
    {
        return repoBase.query(
            "SELECT id_user, name FROM user ORDER BY name",
            rs -> User.of(rs.getInt(1), rs.getString(2)),
            repoBase::mapToStream
        );
    }

    private static IO<Failure, User> addOneEmail(final User user)
    {
        return querySingleEmail(user)
            .map(user::addEmail)
            ;
    }

    private static IO<Failure, User> addEmails(final User user)
    {
        return queryEmails(user)
            .map(StreamUtil.reduce(user, User::addEmail))
            ;
    }

    private static IO<Failure, Email> querySingleEmail(
        final User user
    )
    {
        return repoBase.querySingle(
            "SELECT email, validated " +
                "FROM email " +
                "WHERE id_user=? " +
                "ORDER BY importance desc, validated desc",
            rs -> Email.of(rs.getString(1), rs.getBoolean(2)),
            user.getId()
        );
    }

    private static IO<Failure, Stream<Email>> queryEmails(
        final User user
    )
    {
        return repoBase.query(
            "SELECT email, validated " +
                "FROM email " +
                "WHERE id_user=? " +
                "ORDER BY importance desc, validated desc",
            rs -> Email.of(rs.getString(1), rs.getBoolean(2)),
            repoBase::mapToStream,
            user.getId()
        );
    }

    private static IO<Failure, Stream<Integer>> queryUserIds()
    {
        return repoBase.query(
            "SELECT id_user FROM user ORDER BY name",
            rs -> rs.getInt(1),
            repoBase::mapToStream
        );
    }

    private static boolean checkUserIdSlow(final Integer id)
    {
        return (id == 2);
    }

    private static IO<Failure, Either<Failure, User>> querySingleUser(
        final Optional<Integer> idOpt
    )
    {
        if (idOpt.isPresent()) {
            return repoBase.querySingle(
                "SELECT id_user, name FROM user where id_user = ?",
                rs -> User.of(rs.getInt(1), rs.getString(2)),
                idOpt.get()
            );
        } else {
            return IO.fail(
                Cause.fail(
                    GeneralFailure.of("Missing user id")
                )
            );
        }
    }

    private static <T> void checkDbCommand(
        final IO<Failure, T> testDbCommand
    )
    {
        final Either<Failure, T> repositoryOrFailure = createRepository()
            .flatMap(repository ->
                Cause.resultFlatten(defaultRuntime.unsafeRun(
                    repoBase.use(testDbCommand)
                        .provide(repoBase.name, Repository.Service.class, repository)
                )));

        assertTrue(
            repositoryOrFailure.toString(),
            repositoryOrFailure.isRight()
        );
    }

    private interface User
    {
        static Either<Failure, User> of(
            final int id,
            final String name
        )
        {
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

        User addEmail(final Email email);

        int getId();
    }

    private static class TempUser implements User
    {
        private final int id;

        private final String name;

        private final Optional<TempEmail> email;

        private TempUser(
            final int id,
            final String name,
            final Optional<TempEmail> email
        )
        {
            this.id = id;
            this.name = name;
            this.email = email;
        }

        public User addEmail(Email email)
        {
            if (email instanceof TempEmail) {
                return new TempUser(id, name, Optional.of((TempEmail) email));
            } else if (email instanceof ValidatedEmail) {
                return new ValidatedUser(id, name, (ValidatedEmail) email);
            } else {
                return this;
            }
        }

        public int getId()
        {
            return id;
        }

        @Override
        public String toString()
        {
            return "TempUser(" + id + ", " + name + ", " + email + ")";
        }

        @Override
        public boolean equals(final Object other)
        {
            if (other instanceof TempUser user) {
                return Objects.equals(id, user.id)
                    && Objects.equals(name, user.name)
                    && Objects.equals(email, user.email);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, name, email);
        }
    }

    private static class ValidatedUser implements User
    {
        private final int id;

        private final String name;

        private final ValidatedEmail email;

        private ValidatedUser(
            final int id,
            final String name,
            final ValidatedEmail email
        )
        {
            this.id = id;
            this.name = name;
            this.email = email;
        }

        public User addEmail(Email email)
        {
            return this;
        }

        public int getId()
        {
            return id;
        }

        @Override
        public String toString()
        {
            return "ValidatedUser(" + id + ", " + name + ", " + email + ")";
        }

        @Override
        public boolean equals(final Object other)
        {
            if (other instanceof ValidatedUser user) {
                return Objects.equals(id, user.id)
                    && Objects.equals(name, user.name)
                    && Objects.equals(email, user.email);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(id, name, email);
        }
    }

    private interface Email
    {
        static Email of(final String email, final boolean validated)
        {
            if (validated) {
                return new ValidatedEmail(email);
            } else {
                return new TempEmail(email);
            }
        }
    }

    private static class TempEmail implements Email
    {
        private final String email;

        private TempEmail(final String email)
        {
            this.email = email;
        }

        @Override
        public String toString()
        {
            return "TempEmail(" + email + ")";
        }

        @Override
        public boolean equals(final Object other)
        {
            if (other instanceof TempEmail tempEmail) {
                return Objects.equals(email, tempEmail.email);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(email);
        }
    }

    private static class ValidatedEmail implements Email
    {
        private final String email;

        private ValidatedEmail(final String email)
        {
            this.email = email;
        }

        @Override
        public String toString()
        {
            return "ValidatedEmail(" + email + ")";
        }

        @Override
        public boolean equals(final Object other)
        {
            if (other instanceof ValidatedEmail validatedEmail) {
                return Objects.equals(email, validatedEmail.email);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(email);
        }
    }
}
