package fp.jadobec;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ContactTest
{
    final static DefaultPlatform platform = new DefaultPlatform();

    final static Runtime defaultRuntime =
        new DefaultRuntime(null, platform);

    @AfterAll
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
            Right.of(new TempUserWithEmail(2, "Jane Doe",
                new TempEmail("jane@doe.com"))),
            Right.of(new ValidatedUser(1, "John Doe",
                new ValidatedEmail("john@doe.com")))
        );

        checkDbCommand(
            createAndFill.flatMap(v ->
                Repository.mapStreamEither(
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
                Repository.mapStreamEither(
                    queryUsers(),
                    ContactTest::addOneEmail
                ).map(items -> items.noneMatch(Either::isRight))
            ).peek(Assertions::assertFalse)
        );
    }

    @Test
    public void testContacts()
    {
        final List<Either<Failure, User>> expectedUsers = Arrays.asList(
            Right.of(new TempUser(3, "Jake Doe")),
            Right.of(new ValidatedUser(2, "Jane Doe",
                new ValidatedEmail("jane.doe@doe.com"))),
            Right.of(new ValidatedUser(1, "John Doe",
                new ValidatedEmail("john@doe.com")))
        );

        checkDbCommand(
            createAndFill.flatMap(v ->
                Repository.mapStreamEither(
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
                .flatMap(idOpt ->
                    idOpt.map(IO::succeed)
                        .orElseGet(() -> IO.fail(Cause.fail("Missing user id")))
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
        Repository.transaction(
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

    private static IO<Failure, Integer> insertData()
    {
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

    private static IO<Failure, Stream<Either<Failure, User>>> queryUsers()
    {
        return Repository.query(
            "SELECT id_user, name FROM user ORDER BY name",
            rs -> User.of(rs.getInt(1), rs.getString(2)),
            Repository::mapToStream
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
        return Repository.querySingle(
            "SELECT email, validated " +
                "FROM email " +
                "WHERE id_user=? " +
                "ORDER BY importance desc, validated desc",
            rs -> Email.of(rs.getString(1), rs.getBoolean(2)),
            user.id()
        );
    }

    private static IO<Failure, Stream<Email>> queryEmails(
        final User user
    )
    {
        return Repository.query(
            "SELECT email, validated " +
                "FROM email " +
                "WHERE id_user=? " +
                "ORDER BY importance desc, validated desc",
            rs -> Email.of(rs.getString(1), rs.getBoolean(2)),
            Repository::mapToStream,
            user.id()
        );
    }

    private static IO<Failure, Stream<Integer>> queryUserIds()
    {
        return Repository.query(
            "SELECT id_user FROM user ORDER BY name",
            rs -> rs.getInt(1),
            Repository::mapToStream
        );
    }

    private static boolean checkUserIdSlow(final Integer id)
    {
        return (id == 2);
    }

    private static IO<Failure, Either<Failure, User>> querySingleUser(
        final Integer id
    )
    {
        return Repository.querySingle(
            "SELECT id_user, name FROM user where id_user = ?",
            rs -> User.of(rs.getInt(1), rs.getString(2)),
            id
        );
    }

    private static <T> void checkDbCommand(
        final IO<Failure, T> testDbCommand
    )
    {
        final Either<Failure, T> repositoryOrFailure = createRepository()
            .flatMap(repository ->
                Cause.resultFlatten(defaultRuntime.unsafeRun(
                    Repository.use(testDbCommand)
                        .provide(Repository.Service.class, repository)
                )));

        assertTrue(
            repositoryOrFailure.isRight(),
            repositoryOrFailure.toString()
        );
    }

    private interface User
    {
        static Either<Failure, User> of(
            final int id,
            final String name
        )
        {
            return of(id, name, new NoEmail());
        }

        static Either<Failure, User> of(
            final int id,
            final String name,
            final Email email
        )
        {
            if (name.trim().isEmpty()) {
                return Left.of(
                    GeneralFailure.of("Wrong user name")
                );
            } else {
                return Right.of(
                    switch (email) {
                        case ValidatedEmail validatedEmail -> new ValidatedUser(id, name, validatedEmail);
                        case TempEmail tempEmail -> new TempUserWithEmail(id, name, tempEmail);
                        default -> new TempUser(id, name);
                    }
                );
            }
        }

        User addEmail(final Email email);

        int id();
    }

    private record TempUser(int id, String name)
        implements User
    {
        public User addEmail(Email email)
        {
            if (email instanceof TempEmail tempEmail) {
                return new TempUserWithEmail(id, name, tempEmail);
            } else if (email instanceof ValidatedEmail validatedEmail) {
                return new ValidatedUser(id, name, validatedEmail);
            } else {
                return this;
            }
        }
    }

    private record TempUserWithEmail(int id, String name, TempEmail email) implements User
    {
        @Override
        public User addEmail(Email email)
        {
            if (email instanceof TempEmail tempEmail) {
                return new TempUserWithEmail(id, name, tempEmail);
            } else if (email instanceof ValidatedEmail validatedEmail) {
                return new ValidatedUser(id, name, validatedEmail);
            } else {
                return this;
            }
        }
    }

    private record ValidatedUser(int id, String name, ValidatedEmail validatedEmail)
        implements User
    {
        public User addEmail(Email email)
        {
            return this;
        }
    }

    private interface Email
    {
        static Email of(final String email, final boolean validated)
        {
            if (email == null || email.isBlank()) {
                return new NoEmail();
            } else if (validated) {
                return new ValidatedEmail(email);
            } else {
                return new TempEmail(email);
            }
        }
    }

    private record NoEmail() implements Email
    {
    }

    private record TempEmail(String email) implements Email
    {
    }

    private record ValidatedEmail(String email)
        implements Email
    {
    }
}
