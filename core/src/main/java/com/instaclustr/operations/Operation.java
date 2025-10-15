package com.instaclustr.operations;

import java.io.Closeable;
import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.google.common.base.MoreObjects;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.instaclustr.guice.GuiceInjectorHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

@SuppressWarnings("WeakerAccess")
@JsonTypeInfo(use = JsonTypeInfo.Id.CUSTOM, property = "type", include = As.EXISTING_PROPERTY)
@JsonTypeIdResolver(Operation.TypeIdResolver.class)
public abstract class Operation<RequestT extends OperationRequest> implements Runnable, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Operation.class);

    @JsonIgnore
    private final AtomicBoolean shouldCancel = new AtomicBoolean(false);

    @JsonIgnore
    public String type;

    public static class TypeIdResolver extends MapBackedTypeIdResolver<Operation> {

        public TypeIdResolver() {
            this(GuiceInjectorHolder.INSTANCE.getInjector().getInstance(Key.get(
                new TypeLiteral<Map<String, Class<? extends Operation>>>() {
                }
            )));
        }

        @Inject
        public TypeIdResolver(final Map<String, Class<? extends Operation>> typeMappings) {
            super(typeMappings);
        }
    }

    public static class Error {

        public String source;
        public String message;

        @JsonIgnore
        public Throwable throwable;

        public Error() {

        }

        public Error(final Throwable throwable, final String message, final String source) {
            this.throwable = throwable;

            if (this.throwable != null && this.throwable.getCause() != null && this.throwable.getCause().getMessage() != null) {
                this.message = this.throwable.getCause().getMessage();
            } else {
                this.message = message;
            }

            this.source = source;
        }

        public static Error from(final String source, final Throwable t) {
            return new Error(t, t.getMessage(), source);
        }

        public static Error from(final String source, final Throwable t, final String message) {
            return new Error(t, message, source);
        }

        public static Error from(final Throwable t, final String message) {
            try {
                return new Error(t, message, InetAddress.getLocalHost().getHostName());
            } catch (final Exception ex) {
                throw new RuntimeException("Unable to resolve hostname of this node.");
            }
        }

        public static Error from(final Throwable t) {
            return Error.from(t, t.getMessage());
        }

        public static List<Error> combine(final List<Error> firstErrors, final List<Error> secondErrors) {
            return new ArrayList<Error>() {{
                addAll(firstErrors);
                addAll(secondErrors);
            }};
        }

        public String getSource() {
            return source;
        }

        public void setSource(final String source) {
            this.source = source;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(final String message) {
            this.message = message;
        }

        public Throwable getThrowable() {
            return throwable;
        }

        public void setThrowable(final Throwable throwable) {
            this.throwable = throwable;
        }

        public void report() {
            if (throwable != null) {
                logger.error("--- start of error reporting ---");
                logger.error(toString());
                throwable.printStackTrace();
                logger.error("--- end of error reporting ---");
            } else {
                logger.info(toString());
            }
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("source", source)
                .add("message", message)
                .add("throwable", throwable)
                .toString();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Error error = (Error) o;
            return Objects.equals(source, error.source) &&
                Objects.equals(message, error.message) &&
                Objects.equals(throwable, error.throwable);
        }

        @Override
        public int hashCode() {
            return Objects.hash(source, message, throwable);
        }
    }

    public enum State {
        PENDING, RUNNING, COMPLETED, CANCELLED, FAILED;

        public static Set<State> TERMINAL_STATES = EnumSet.of(COMPLETED, FAILED, CANCELLED);

        public boolean isTerminalState() {
            return TERMINAL_STATES.contains(this);
        }
    }

    public UUID id = UUID.randomUUID();
    public Instant creationTime = Instant.now();

    @JsonUnwrapped // embed the request parameters in the serialized output directly
    @JsonProperty(access = JsonProperty.Access.READ_ONLY) // added so unwrap works ok with JsonCreator
    public RequestT request;

    public State state = State.PENDING;
    public List<Error> errors = new ArrayList<>();
    public float progress = 0;
    public Instant startTime, completionTime;

    public Operation(final RequestT request) {
        this.request = request;
    }

    protected Operation(final String type,
                        final UUID id,
                        final Instant creationTime,
                        final State state,
                        final List<Error> errors,
                        final float progress,
                        final Instant startTime,
                        final RequestT request) {
        this.id = id;
        this.creationTime = creationTime;
        this.state = state;
        this.progress = progress;
        this.startTime = startTime;
        this.request = request;
        this.type = type;
        if (errors != null) {
            this.errors.addAll(errors);
        }
    }

    @JsonIgnore
    public boolean hasErrors() {
        return this.errors != null && !this.errors.isEmpty();
    }

    @JsonIgnore
    public void addError(final Error error) {
        if (this.errors != null && error != null) {
            this.errors.add(error);
        }
    }

    @JsonIgnore
    public void addErrors(final List<Error> errors) {
        if (this.errors != null && errors != null) {
            for (final Error e : errors) {
                addError(e);
            }
        }
    }

    @Override
    public final void run() {
        state = State.RUNNING;
        startTime = Instant.now();

        try {
            run0();
            state = State.COMPLETED;

        } catch (final Throwable t) {
            if (shouldCancel.get()) {
                logger.warn("Operation %s was cancelled.");
                state = State.CANCELLED;
            } else {
                logger.error(format("Operation %s has failed.", id), t);
                state = State.FAILED;
            }

            if (errors == null) {
                errors = new ArrayList<>();
            }

            errors.add(Error.from(t));
        } finally {
            if (this.errors != null && !this.errors.isEmpty()) {
                state = State.FAILED;
            }
            progress = 1;
            completionTime = Instant.now();
        }

        if (this.errors != null && !this.errors.isEmpty()) {
            logger.error("Reporting errors for operation {}", id);
            for (final Error error : errors) {
                error.report();
            }
        }
    }

    protected abstract void run0() throws Exception;

    public void close() {
        shouldCancel.set(true);
    }

    public AtomicBoolean getShouldCancel() {
        return shouldCancel;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("creationTime", creationTime)
            .add("request", request)
            .add("state", state)
            .add("progress", progress)
            .add("startTime", startTime)
            .add("completionTime", completionTime)
            .add("errors", errors)
            .add("shouldCancel", shouldCancel.get())
            .toString();
    }
}
