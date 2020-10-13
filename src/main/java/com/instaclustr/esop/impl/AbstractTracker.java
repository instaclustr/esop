package com.instaclustr.esop.impl;

import static com.instaclustr.esop.impl.AbstractTracker.Unit.State.CANCELLED;
import static com.instaclustr.esop.impl.AbstractTracker.Unit.State.FAILED;
import static com.instaclustr.esop.impl.AbstractTracker.Unit.State.FINISHED;
import static com.instaclustr.esop.impl.AbstractTracker.Unit.State.IGNORED;
import static com.instaclustr.esop.impl.AbstractTracker.Unit.State.NOT_STARTED;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.instaclustr.esop.impl.AbstractTracker.Session;
import com.instaclustr.esop.impl.AbstractTracker.Unit;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationRequest;
import com.instaclustr.operations.OperationsService;
import com.instaclustr.threading.Executors.FixedTasksExecutorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTracker<UNIT extends Unit, SESSION extends Session<UNIT>, INTERACTOR extends StorageInteractor, REQUEST extends OperationRequest> extends AbstractIdleService {

    protected Logger logger = LoggerFactory.getLogger(AbstractTracker.class);

    protected final ListeningExecutorService finisherExecutorService;
    protected final OperationsService operationsService;

    protected final List<UNIT> units = Collections.synchronizedList(new ArrayList<>());
    protected final Set<Session<UNIT>> sessions = Collections.synchronizedSet(new HashSet<>());

    public AtomicLong submittedUnits = new AtomicLong(0);
    public AtomicLong submittedSessions = new AtomicLong(0);

    public AbstractTracker(final ListeningExecutorService finisherExecutorService,
                           final OperationsService operationsService) {
        this.finisherExecutorService = finisherExecutorService;
        this.operationsService = operationsService;
    }

    @Override
    protected void startUp() throws Exception {
        logger.info("Starting tracker ...");
    }

    @Override
    protected void shutDown() throws Exception {
        logger.info("Executor service terminating, shutting down finisher executor service ...");

        finisherExecutorService.shutdown();

        while (true) {
            logger.info("Waiting until all submitted tasks were terminated ...");
            if (finisherExecutorService.awaitTermination(5, MINUTES)) {
                break;
            }
        }

        logger.info("Finisher executor service terminated.");
    }

    public abstract UNIT constructUnitToSubmit(final INTERACTOR interactor,
                                               final ManifestEntry manifestEntry,
                                               final AtomicBoolean shouldCancel,
                                               final String snapshotTag);

    public abstract Session<UNIT> constructSession();

    public synchronized Session<UNIT> submit(final INTERACTOR interactor,
                                             final Operation<? extends REQUEST> operation,
                                             final Collection<ManifestEntry> entries,
                                             final String snapshotTag,
                                             final int concurrentConnections) {

        final Session<UNIT> currentSession = constructSession();
        currentSession.setSnapshotTag(snapshotTag);
        currentSession.setId(operation.id);

        if (entries.isEmpty()) {
            logger.info("0 files to process.");
            return currentSession;
        }

        // we have executor service per request in order to specify maximal
        // concurrent uploads, if we had one global executor, we could not "cap it".
        final ListeningExecutorService executorService = new FixedTasksExecutorSupplier().get(concurrentConnections);

        final Map<ListenableFuture<Void>, Unit> futures = new HashMap<>();

        for (final ManifestEntry entry : entries) {

            UNIT alreadySubmitted = null;

            final Iterator<UNIT> it = Collections.unmodifiableList(new ArrayList<>(units)).iterator();

            while (it.hasNext()) {
                UNIT unit = it.next();

                if (unit.getManifestEntry().objectKey.equals(entry.objectKey)) {
                    alreadySubmitted = unit;
                    break;
                }
            }

            if (alreadySubmitted == null) {
                final UNIT unit = constructUnitToSubmit(interactor, entry, operation.getShouldCancel(), snapshotTag);

                units.add(unit);
                futures.put(executorService.submit(unit), unit);

                submittedUnits.incrementAndGet();

                currentSession.addUnit(unit);
            } else {
                logger.info(String.format("Session %s skips as already submitted: %s", currentSession.getId(), alreadySubmitted.getManifestEntry().objectKey));
                currentSession.addUnit(alreadySubmitted);
            }
        }

        sessions.add(currentSession);
        submittedSessions.incrementAndGet();

        futures.forEach((key, value) -> key.addListener(() -> {
            synchronized (sessions) {
                // increment finished units across all sessions
                sessions.stream().filter(s -> s.getUnits().contains(value)).forEach(s -> {
                    operationsService.operation(s.getId()).ifPresent(op -> {
                        s.finishedUnits.incrementAndGet();
                        logger.debug(String.format("Progress of upload operation %s: %s", op.id, s.getProgress()));
                        op.progress = s.getProgress();
                    });
                });

                units.remove(value);
            }
        }, finisherExecutorService));

        currentSession.setExecutorService(executorService);
        return currentSession;
    }

    public int numberOfUnits() {
        return units.size();
    }

    public void removeSession(final Session<?> session) {
        if (session != null) {
            session.clear();
            sessions.remove(session);
        }
    }

    public Optional<Session<UNIT>> getSession(final String sessionId) {
        return getSession(UUID.fromString(sessionId));
    }

    public Optional<Session<UNIT>> getSession(final UUID sessionId) {
        return sessions.stream().filter(s -> s.getId().equals(sessionId)).findFirst();
    }

    public List<UNIT> getUnits() {
        return Collections.unmodifiableList(units);
    }

    public Set<Session<UNIT>> getSessions() {
        return Collections.unmodifiableSet(sessions);
    }

    public void cancelIfNecessary(final Session<? extends Unit> session) {
        if (session.isSuccessful()) {
            return;
        }

        // Non-failed unit is an unit which has not started yet
        // or it runs without an error so far.
        // Not-started unit is submitted to executor but it has not been executed yet,
        // the most probably because it waits until it fits into pool
        session.getNonFailedUnits().forEach(unit -> {
            if (unit.getState() == NOT_STARTED) {
                logger.info(format("Ignoring %s from processing because there was an errorneous unit in a session %s",
                                   unit.getManifestEntry().localFile,
                                   session.id));
                unit.setState(IGNORED);
            } else if (unit.getState() == Unit.State.RUNNING) {
                logger.info(format("Cancelling %s because there was an errorneous unit in a session %s",
                                   unit.getManifestEntry().localFile,
                                   session.id));
                unit.setState(CANCELLED);
                unit.shouldCancel.set(true);
            }
        });
    }

    public static abstract class Unit implements java.util.concurrent.Callable<Void> {

        @JsonIgnore
        protected String snapshotTag;
        protected final ManifestEntry manifestEntry;
        protected volatile State state = NOT_STARTED;
        protected Throwable throwable = null;
        @JsonIgnore
        protected final AtomicBoolean shouldCancel;

        public Unit(final ManifestEntry manifestEntry,
                    final AtomicBoolean shouldCancel) {
            this.manifestEntry = manifestEntry;
            this.shouldCancel = shouldCancel;
        }

        public enum State {
            NOT_STARTED,
            RUNNING,
            FINISHED,
            FAILED,
            IGNORED,
            CANCELLED
        }

        public String getSnapshotTag() {
            return snapshotTag;
        }

        public void setSnapshotTag(final String snapshotTag) {
            this.snapshotTag = snapshotTag;
        }

        public ManifestEntry getManifestEntry() {
            return manifestEntry;
        }

        public void setState(final State state) {
            this.state = state;
        }

        public State getState() {
            return state;
        }

        public boolean isErroneous() {
            return throwable != null;
        }

        public Throwable getThrowable() {
            return throwable;
        }

        @JsonIgnore
        public AtomicBoolean getShouldCancel() {
            return shouldCancel;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Unit unit = (Unit) o;
            return Objects.equal(manifestEntry, unit.manifestEntry);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(manifestEntry);
        }
    }

    public static abstract class Session<U extends Unit> {

        @JsonIgnore
        protected ListeningExecutorService executorService;

        private static final Logger logger = LoggerFactory.getLogger(Session.class);

        protected String snapshotTag;
        protected UUID id;
        protected final List<U> units = Collections.synchronizedList(new ArrayList<>());

        public final AtomicLong submittedUnits = new AtomicLong(0);
        public final AtomicLong finishedUnits = new AtomicLong(0);

        public void setId(final UUID id) {
            this.id = id;
        }

        public UUID getId() {
            return id;
        }

        public String getSnapshotTag() {
            return snapshotTag;
        }

        public void setSnapshotTag(final String snapshotTag) {
            this.snapshotTag = snapshotTag;
        }

        public List<U> getUnits() {
            return units;
        }

        public synchronized boolean isConsideredFinished() {
            return units.stream().anyMatch(unit -> unit.getState() == FAILED) ||
                units.stream().allMatch(unit -> unit.getState() == FINISHED);
        }

        public synchronized boolean isSuccessful() {
            return units.stream().noneMatch(unit -> unit.getState() == FAILED);
        }

        @JsonIgnore
        public List<U> getFailedUnits() {
            if (isSuccessful()) {
                return Collections.emptyList();
            }

            return units.stream().filter(unit -> unit.getState() == FAILED).collect(toList());
        }

        public List<U> getNonFailedUnits() {
            return units.stream().filter(unit -> unit.getState() != FAILED).collect(toList());
        }

        public void setExecutorService(final ListeningExecutorService executorService) {
            this.executorService = executorService;
        }

        public synchronized float getProgress() {
            if (submittedUnits.get() == 0) {
                return 0;
            } else {
                return finishedUnits.get() / (float) submittedUnits.get();
            }
        }

        @JsonIgnore
        public void waitUntilConsideredFinished() {
            await().pollInSameThread().forever().pollInterval(5, SECONDS).until(this::isConsideredFinished);

            if (executorService != null) {
                executorService.shutdown();
                await().pollInSameThread().forever().pollInterval(5, SECONDS).until(() -> executorService.isTerminated());
            }

            logger.info(format("%sSession %s has finished %s",
                               snapshotTag != null ? "Snapshot " + snapshotTag + " - " : "",
                               id,
                               isSuccessful() ? "successfully" : "errorneously"));
        }

        public void addUnit(final U unit) {
            units.add(unit);
            submittedUnits.incrementAndGet();
        }

        public void clear() {
            units.clear();
        }
    }
}
