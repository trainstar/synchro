-- Synthetic training-application schema for realistic sync data.
-- The shape follows a real consumer: tenants, members, a shared exercise
-- catalog with a stored generated search column, private and shared programs,
-- parent/child and many-to-many rows, application triggers that write other
-- registered tables, soft deletes, defaults, and portable value boundaries.
-- It contains no real user data.

CREATE TABLE organizations (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    settings JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE organization_members (
    id UUID PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations (id),
    user_id TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'athlete' CHECK (role IN ('owner', 'coach', 'athlete')),
    joined_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ,
    UNIQUE (organization_id, user_id)
);

CREATE INDEX organization_members_user_id_idx ON organization_members (user_id);

CREATE TABLE equipment (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

-- A NULL organization_id marks a global catalog exercise.
CREATE TABLE exercises (
    id UUID PRIMARY KEY,
    organization_id UUID REFERENCES organizations (id),
    name TEXT NOT NULL,
    instructions TEXT NOT NULL DEFAULT '',
    muscle_groups TEXT[] NOT NULL DEFAULT '{}',
    difficulty SMALLINT NOT NULL DEFAULT 2,
    metrics JSONB NOT NULL DEFAULT '{}',
    search_text TEXT NOT NULL DEFAULT '',
    search_vector TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', name || ' ' || search_text)) STORED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX exercises_organization_id_idx ON exercises (organization_id);

CREATE TABLE exercise_equipment (
    id UUID PRIMARY KEY,
    exercise_id UUID NOT NULL REFERENCES exercises (id),
    equipment_id UUID NOT NULL REFERENCES equipment (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ,
    UNIQUE (exercise_id, equipment_id)
);

CREATE INDEX exercise_equipment_exercise_id_idx ON exercise_equipment (exercise_id);

CREATE TABLE programs (
    id UUID PRIMARY KEY,
    organization_id UUID NOT NULL REFERENCES organizations (id),
    owner_id TEXT NOT NULL DEFAULT current_setting('synchro.user_id', true),
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    visibility TEXT NOT NULL DEFAULT 'private' CHECK (visibility IN ('private', 'organization')),
    week_count INTEGER NOT NULL DEFAULT 4,
    settings JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX programs_owner_id_idx ON programs (owner_id);
CREATE INDEX programs_organization_id_idx ON programs (organization_id);

CREATE TABLE workouts (
    id UUID PRIMARY KEY,
    program_id UUID NOT NULL REFERENCES programs (id),
    owner_id TEXT NOT NULL DEFAULT current_setting('synchro.user_id', true),
    name TEXT NOT NULL,
    notes TEXT NOT NULL DEFAULT '',
    scheduled_on DATE,
    started_at TIMESTAMPTZ,
    duration_seconds INTEGER,
    duration_minutes INTEGER GENERATED ALWAYS AS (duration_seconds / 60) STORED,
    total_volume_kg NUMERIC(12,2) NOT NULL DEFAULT 0,
    external_ref BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX workouts_program_id_idx ON workouts (program_id);

-- program_id is a trigger-maintained copy of the parent workout program.
CREATE TABLE workout_exercises (
    id UUID PRIMARY KEY,
    workout_id UUID NOT NULL REFERENCES workouts (id),
    program_id UUID,
    exercise_id UUID NOT NULL REFERENCES exercises (id),
    owner_id TEXT NOT NULL DEFAULT current_setting('synchro.user_id', true),
    position INTEGER NOT NULL,
    target JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX workout_exercises_workout_id_idx ON workout_exercises (workout_id);
CREATE INDEX workout_exercises_program_id_idx ON workout_exercises (program_id);

-- workout_id and program_id are trigger-maintained copies of the parent chain.
CREATE TABLE exercise_sets (
    id UUID PRIMARY KEY,
    workout_exercise_id UUID NOT NULL REFERENCES workout_exercises (id),
    workout_id UUID,
    program_id UUID,
    owner_id TEXT NOT NULL DEFAULT current_setting('synchro.user_id', true),
    set_index SMALLINT NOT NULL,
    reps INTEGER NOT NULL DEFAULT 0,
    weight_kg NUMERIC(7,2) NOT NULL DEFAULT 0,
    rpe DOUBLE PRECISION,
    duration_ms BIGINT,
    completed_at TIMESTAMPTZ,
    note TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX exercise_sets_workout_exercise_id_idx ON exercise_sets (workout_exercise_id);
CREATE INDEX exercise_sets_workout_id_idx ON exercise_sets (workout_id);
CREATE INDEX exercise_sets_program_id_idx ON exercise_sets (program_id);

CREATE TABLE workout_media (
    id UUID PRIMARY KEY,
    workout_id UUID NOT NULL REFERENCES workouts (id),
    owner_id TEXT NOT NULL DEFAULT current_setting('synchro.user_id', true),
    content_type TEXT NOT NULL,
    body BYTEA NOT NULL,
    byte_size INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX workout_media_workout_id_idx ON workout_media (workout_id);

-- Same-row BEFORE trigger: denormalized lowercase search text. The builtin
-- pg_c_utf8 collation makes the Unicode case mapping independent of the
-- database locale.
CREATE FUNCTION exercises_search_text() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
    NEW.search_text := lower((NEW.name || ' ' || array_to_string(NEW.muscle_groups, ' ')) COLLATE pg_c_utf8);
    RETURN NEW;
END;
$$;

CREATE TRIGGER exercises_search_text
BEFORE INSERT OR UPDATE OF name, muscle_groups ON exercises
FOR EACH ROW EXECUTE FUNCTION exercises_search_text();

-- Same-row BEFORE triggers: copy the parent chain onto child rows.
CREATE FUNCTION workout_exercises_parent() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
    SELECT workout.program_id INTO STRICT NEW.program_id
    FROM workouts AS workout WHERE workout.id = NEW.workout_id;
    RETURN NEW;
END;
$$;

CREATE TRIGGER workout_exercises_parent
BEFORE INSERT OR UPDATE OF workout_id ON workout_exercises
FOR EACH ROW EXECUTE FUNCTION workout_exercises_parent();

CREATE FUNCTION exercise_sets_parent() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
    SELECT parent.workout_id, parent.program_id INTO STRICT NEW.workout_id, NEW.program_id
    FROM workout_exercises AS parent WHERE parent.id = NEW.workout_exercise_id;
    RETURN NEW;
END;
$$;

CREATE TRIGGER exercise_sets_parent
BEFORE INSERT OR UPDATE OF workout_exercise_id ON exercise_sets
FOR EACH ROW EXECUTE FUNCTION exercise_sets_parent();

CREATE FUNCTION workout_media_size() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
    NEW.byte_size := octet_length(NEW.body);
    RETURN NEW;
END;
$$;

CREATE TRIGGER workout_media_size
BEFORE INSERT OR UPDATE OF body ON workout_media
FOR EACH ROW EXECUTE FUNCTION workout_media_size();

-- Different-row AFTER triggers. A set change recomputes its workout volume.
-- A workout change touches its program. The set trigger name sorts before
-- synchro_capture_fence, and the workout trigger name sorts after it.
CREATE FUNCTION exercise_sets_rollup() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE
    target uuid := CASE WHEN TG_OP = 'DELETE' THEN OLD.workout_id ELSE NEW.workout_id END;
BEGIN
    UPDATE workouts AS workout
    SET total_volume_kg = (
            SELECT COALESCE(sum(item.reps * item.weight_kg), 0)
            FROM exercise_sets AS item
            WHERE item.workout_id = target AND item.deleted_at IS NULL
        ),
        updated_at = clock_timestamp()
    WHERE workout.id = target;
    RETURN NULL;
END;
$$;

CREATE TRIGGER exercise_sets_rollup
AFTER INSERT OR UPDATE OF reps, weight_kg, deleted_at OR DELETE ON exercise_sets
FOR EACH ROW EXECUTE FUNCTION exercise_sets_rollup();

CREATE FUNCTION workouts_touch_program() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
BEGIN
    UPDATE programs SET updated_at = clock_timestamp()
    WHERE id = CASE WHEN TG_OP = 'DELETE' THEN OLD.program_id ELSE NEW.program_id END;
    RETURN NULL;
END;
$$;

CREATE TRIGGER workouts_touch_program
AFTER INSERT OR UPDATE OR DELETE ON workouts
FOR EACH ROW EXECUTE FUNCTION workouts_touch_program();
