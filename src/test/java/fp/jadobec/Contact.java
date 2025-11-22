package fp.jadobec;

import fp.util.Either;
import fp.util.Failure;
import fp.util.GeneralFailure;
import fp.util.Left;
import fp.util.Right;

public class Contact
{
    public interface User
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
            if (id < 0) {
                return Left.of(
                    GeneralFailure.of("Wrong user id")
                );
            } else if (name == null || name.isBlank()) {
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

    public record TempUser(int id, String name)
        implements User
    {
        public TempUser
        {
            if (id < 0) {
                throw new IllegalArgumentException("Wrong user id");
            }
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("Wrong user name");
            }
        }

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

    public record TempUserWithEmail(int id, String name, TempEmail email) implements User
    {
        public TempUserWithEmail
        {
            if (id < 0) {
                throw new IllegalArgumentException("Wrong user id");
            }
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("Wrong user name");
            }
            if (email == null) {
                throw new IllegalArgumentException("Wrong email");
            }
        }

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

    public record ValidatedUser(int id, String name, ValidatedEmail validatedEmail)
        implements User
    {
        public ValidatedUser
        {
            if (id < 0) {
                throw new IllegalArgumentException("Wrong user id");
            }
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("Wrong user name");
            }
            if (validatedEmail == null) {
                throw new IllegalArgumentException("Wrong email");
            }
        }

        public User addEmail(Email email)
        {
            return this;
        }
    }

    public interface Email
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

    public record NoEmail() implements Email
    {
    }

    public record TempEmail(String email) implements Email
    {
        public TempEmail
        {
            if (email == null || email.isBlank()) {
                throw new IllegalArgumentException("Wrong email");
            }
        }
    }

    public record ValidatedEmail(String email) implements Email
    {
        public ValidatedEmail
        {
            if (email == null || email.isBlank()) {
                throw new IllegalArgumentException("Wrong email");
            }
        }
    }
}
