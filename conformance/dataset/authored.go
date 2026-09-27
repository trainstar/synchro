package dataset

import "fmt"

// Authored identities. Each table has its own leading code.
func authoredID(table, index int) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012d", table, index)
}

// Authored user identities. dana starts with no organization.
const (
	Alice = "alice"
	Bob   = "bob"
	Chen  = "chen"
	Dana  = "dana"
)

// AuthoredUsers lists every authored user.
var AuthoredUsers = []string{Alice, Bob, Chen, Dana}

// Authored record identities.
var (
	OrgA = authoredID(1, 1)
	OrgB = authoredID(1, 2)

	MemberAliceA = authoredID(2, 1)
	MemberAliceB = authoredID(2, 2)
	MemberBobA   = authoredID(2, 3)
	MemberChenB  = authoredID(2, 4)
	MemberDanaB  = authoredID(2, 5)

	Barbell    = authoredID(3, 1)
	Kettlebell = authoredID(3, 2)

	BackSquat  = authoredID(4, 1)
	BenchPress = authoredID(4, 2)
	OrgPress   = authoredID(4, 3)

	SquatBarbell = authoredID(5, 1)
	BenchBarbell = authoredID(5, 2)
	PressBell    = authoredID(5, 3)

	ProgramAliceShared  = authoredID(6, 1)
	ProgramAlicePrivate = authoredID(6, 2)
	ProgramBob          = authoredID(6, 3)
	ProgramChen         = authoredID(6, 4)

	WorkoutA1 = authoredID(7, 1)
	WorkoutA2 = authoredID(7, 2)
	WorkoutA3 = authoredID(7, 3)
	WorkoutB1 = authoredID(7, 4)
	WorkoutC1 = authoredID(7, 5)

	EntryA1Squat = authoredID(8, 1)
	EntryA1Bench = authoredID(8, 2)
	EntryA2Press = authoredID(8, 3)
	EntryA3Squat = authoredID(8, 4)
	EntryB1Squat = authoredID(8, 5)
	EntryC1Bench = authoredID(8, 6)

	SetA1Squat1 = authoredID(9, 1)
	SetA1Squat2 = authoredID(9, 2)
	SetA1Squat3 = authoredID(9, 3)
	SetA1Bench1 = authoredID(9, 4)
	SetA2Press1 = authoredID(9, 5)
	SetA3Squat1 = authoredID(9, 6)
	SetB1Squat1 = authoredID(9, 7)
	SetC1Bench1 = authoredID(9, 8)
	SetA1Bench2 = authoredID(9, 9)

	MediaAlice = authoredID(10, 1)
	MediaBob   = authoredID(10, 2)
)

// AuthoredStep is one server-side action of the authored flow. SQL is one
// source transaction. Grants and Revokes are [user, scope] operator actions.
type AuthoredStep struct {
	Name    string
	SQL     string
	Grants  [][2]string
	Revokes [][2]string
}

// AuthoredPush is one client-authored mutation. Columns hold wire values.
type AuthoredPush struct {
	User    string
	Table   string
	ID      string
	Op      string
	Columns map[string]string
}

// ScopeRows maps scope -> table -> record IDs in ascending order.
type ScopeRows map[string]map[string][]string

// ValueExpectation is one hand-written wire value of one source row.
type ValueExpectation struct {
	Table  string
	ID     string
	Column string
	Wire   string
}

// Checkpoint is the complete hand-written expected state after a step.
type Checkpoint struct {
	Scopes   map[string][]string
	Rows     ScopeRows
	Values   []ValueExpectation
	Assigned map[string][]string
}

// mediaBody returns the authored blob bytes, including 0x00 and 0xff.
func mediaBody(seed byte, size int) []byte {
	body := make([]byte, size)
	for index := range body {
		body[index] = byte(index*31) ^ seed
	}
	return body
}

// AuthoredMediaAlice is the authored body of MediaAlice.
var AuthoredMediaAlice = mediaBody(0x89, 300)

// AuthoredMediaBob is the authored body of MediaBob.
var AuthoredMediaBob = mediaBody(0x00, 1024)

// AuthoredSeed is the initial server transaction. Its multi-row statements
// fire the application triggers that poisoned the first real consumer.
var AuthoredSeed = AuthoredStep{
	Name: "seed",
	SQL: fmt.Sprintf(`
INSERT INTO organizations (id, name, slug, settings) VALUES
    ('%[1]s', 'Nordisk Styrke Ålesund', 'nordisk-styrke', '{"units":"kg","locale":"nb-NO"}'),
    ('%[2]s', 'Club Deportivo Montaña 山', 'cd-montana', '{"week_starts":"monday","units":"lb"}');
INSERT INTO organization_members (id, organization_id, user_id, role, joined_at) VALUES
    ('%[3]s', '%[1]s', 'alice', 'owner', '2026-01-05T08:00:00.000001Z'),
    ('%[4]s', '%[2]s', 'alice', 'coach', '2026-01-06T08:00:00Z'),
    ('%[5]s', '%[1]s', 'bob', DEFAULT, '2026-01-07T08:00:00Z'),
    ('%[6]s', '%[2]s', 'chen', 'athlete', '2026-01-08T08:00:00Z');
INSERT INTO equipment (id, name, category) VALUES
    ('%[7]s', 'Barbell', 'free_weight'),
    ('%[8]s', 'Kettlebell 🔔', 'free_weight');
INSERT INTO exercises (id, organization_id, name, muscle_groups, difficulty, metrics) VALUES
    ('%[9]s', NULL, 'Back Squat', '{quadriceps,glutes}', 3, '{"unit":"kg","tracks":["reps","weight"]}'),
    ('%[10]s', NULL, 'Жим лёжа', '{chest,triceps}', DEFAULT, '{"unit":"kg","bar_path":1.25e-1}'),
    ('%[11]s', '%[1]s', 'Überkopfdrücken "strict" \n', '{shoulders}', 4, DEFAULT);
INSERT INTO exercise_equipment (id, exercise_id, equipment_id) VALUES
    ('%[12]s', '%[9]s', '%[7]s'),
    ('%[13]s', '%[10]s', '%[7]s'),
    ('%[14]s', '%[11]s', '%[8]s');
INSERT INTO programs (id, organization_id, owner_id, title, visibility, week_count, settings) VALUES
    ('%[15]s', '%[1]s', 'alice', 'Strength Block 1', 'organization', 6, '{"deload_week":4}'),
    ('%[16]s', '%[2]s', 'alice', 'Personal Rehab', DEFAULT, DEFAULT, DEFAULT),
    ('%[17]s', '%[1]s', 'bob', 'Bob Base', 'private', 8, '{}'),
    ('%[18]s', '%[2]s', 'chen', 'Temporada ⚽', 'organization', 12, '{"goal":"endurance"}');
INSERT INTO workouts (id, program_id, owner_id, name, notes, scheduled_on, started_at, duration_seconds, external_ref) VALUES
    ('%[19]s', '%[15]s', 'alice', 'Heavy Day', 'Tiefe gut ✅', '2026-03-02', '2026-03-02T06:30:00.123456Z', 3725, 9223372036854775807),
    ('%[20]s', '%[15]s', 'alice', 'Press Day', E'line one\nline "two"\\', '2026-03-04', NULL, NULL, -9223372036854775808),
    ('%[21]s', '%[16]s', 'alice', 'Mobility', '', NULL, NULL, 900, NULL),
    ('%[22]s', '%[17]s', 'bob', 'Base A', '膝を外に', '2026-02-28', '2026-02-28T18:00:00Z', 60, 9007199254740993),
    ('%[23]s', '%[18]s', 'chen', 'Sesión 1', 'tab	and "quote"', '2026-03-01', NULL, 120, 0);
INSERT INTO workout_exercises (id, workout_id, exercise_id, owner_id, position, target) VALUES
    ('%[24]s', '%[19]s', '%[9]s', 'alice', 1, '{"sets":3,"reps":[5,5,5]}'),
    ('%[25]s', '%[19]s', '%[10]s', 'alice', 2, '{"sets":1,"tempo":"3-1-1"}'),
    ('%[26]s', '%[20]s', '%[11]s', 'alice', 1, DEFAULT),
    ('%[27]s', '%[21]s', '%[9]s', 'alice', 1, '{}'),
    ('%[28]s', '%[22]s', '%[9]s', 'bob', 1, '{"sets":2}'),
    ('%[29]s', '%[23]s', '%[10]s', 'chen', 1, '{"sets":1}');
INSERT INTO exercise_sets (id, workout_exercise_id, owner_id, set_index, reps, weight_kg, rpe, duration_ms, completed_at, note) VALUES
    ('%[30]s', '%[24]s', 'alice', 1, 5, 100.00, 7.5, 1, '2026-03-02T06:40:00.000001Z', ''),
    ('%[31]s', '%[24]s', 'alice', 2, 5, 102.50, 8, 9007199254740993, '2026-03-02T06:45:00Z', 'solid'),
    ('%[32]s', '%[24]s', 'alice', 3, 5, 105.25, 8.5, NULL, NULL, 'grind 😤'),
    ('%[33]s', '%[25]s', 'alice', 1, 8, 60, 1e-7, -1, NULL, ''),
    ('%[34]s', '%[26]s', 'alice', 1, 6, 40.5, NULL, NULL, NULL, 'Ü'),
    ('%[35]s', '%[27]s', 'alice', 1, 10, 0, 6, 0, NULL, ''),
    ('%[36]s', '%[28]s', 'bob', 1, 3, 80, 9.5, 5000, '2026-02-28T18:20:00Z', ''),
    ('%[37]s', '%[29]s', 'chen', 1, 12, 20.1, 10, 123456789012, NULL, '¡vamos!');
INSERT INTO workout_media (id, workout_id, owner_id, content_type, body) VALUES
    ('%[38]s', '%[19]s', 'alice', 'image/png', '\x%[40]x'),
    ('%[39]s', '%[22]s', 'bob', 'application/octet-stream', '\x%[41]x');
`,
		OrgA, OrgB,
		MemberAliceA, MemberAliceB, MemberBobA, MemberChenB,
		Barbell, Kettlebell,
		BackSquat, BenchPress, OrgPress,
		SquatBarbell, BenchBarbell, PressBell,
		ProgramAliceShared, ProgramAlicePrivate, ProgramBob, ProgramChen,
		WorkoutA1, WorkoutA2, WorkoutA3, WorkoutB1, WorkoutC1,
		EntryA1Squat, EntryA1Bench, EntryA2Press, EntryA3Squat, EntryB1Squat, EntryC1Bench,
		SetA1Squat1, SetA1Squat2, SetA1Squat3, SetA1Bench1, SetA2Press1, SetA3Squat1, SetB1Squat1, SetC1Bench1,
		MediaAlice, MediaBob, AuthoredMediaAlice, AuthoredMediaBob,
	),
	Grants: [][2]string{
		{Alice, "org:" + OrgA}, {Alice, "org:" + OrgB}, {Bob, "org:" + OrgA}, {Chen, "org:" + OrgB},
	},
}

// AuthoredSetPush is the client insert that alice pushes after the seed.
// It authors no owner or parent-chain column. The server fills them.
var AuthoredSetPush = AuthoredPush{
	User:  Alice,
	Table: "exercise_sets",
	ID:    SetA1Bench2,
	Op:    "insert",
	Columns: map[string]string{
		"workout_exercise_id": `"` + EntryA1Bench + `"`,
		"set_index":           `2`,
		"reps":                `10`,
		"weight_kg":           `"62.5"`,
		"rpe":                 `9.5`,
		"duration_ms":         `"-9223372036854775808"`,
		"completed_at":        `"2026-03-02T07:05:00.500000Z"`,
		"note":                `"新記録 🎉 \"PR\"\\"`,
	},
}

// AuthoredHistory is the server history after the push, in order.
var AuthoredHistory = []AuthoredStep{
	{
		Name: "share-private-program",
		SQL:  fmt.Sprintf(`UPDATE programs SET visibility = 'organization', updated_at = clock_timestamp() WHERE id = '%s'`, ProgramAlicePrivate),
	},
	{
		Name: "dana-joins-org-b",
		SQL: fmt.Sprintf(`INSERT INTO organization_members (id, organization_id, user_id, joined_at)
VALUES ('%s', '%s', 'dana', '2026-03-05T09:00:00Z')`, MemberDanaB, OrgB),
		Grants: [][2]string{{Dana, "org:" + OrgB}},
	},
	{
		Name:    "bob-leaves-org-a",
		SQL:     fmt.Sprintf(`UPDATE organization_members SET deleted_at = '2026-03-06T09:00:00Z', updated_at = clock_timestamp() WHERE id = '%s'`, MemberBobA),
		Revokes: [][2]string{{Bob, "org:" + OrgA}},
	},
	{
		Name: "hard-delete-set",
		SQL:  fmt.Sprintf(`DELETE FROM exercise_sets WHERE id = '%s'`, SetA1Squat2),
	},
	{
		Name: "re-create-set",
		SQL: fmt.Sprintf(`INSERT INTO exercise_sets (id, workout_exercise_id, owner_id, set_index, reps, weight_kg, rpe, note)
VALUES ('%s', '%s', 'alice', 2, 3, 110, 9, 're-created')`, SetA1Squat2, EntryA1Squat),
	},
	{
		Name: "multi-row-weight-update",
		SQL: fmt.Sprintf(`UPDATE exercise_sets SET weight_kg = weight_kg + 2.5, updated_at = clock_timestamp()
WHERE workout_exercise_id = '%s'`, EntryA1Squat),
	},
	{
		Name: "rename-catalog-exercise",
		SQL:  fmt.Sprintf(`UPDATE exercises SET name = 'Back Squat (High Bar)', updated_at = clock_timestamp() WHERE id = '%s'`, BackSquat),
	},
	{
		Name: "delete-bob-program",
		SQL:  fmt.Sprintf(`UPDATE programs SET deleted_at = '2026-03-07T10:00:00Z', updated_at = clock_timestamp() WHERE id = '%s'`, ProgramBob),
	},
}

func scopeRows(entries ...any) map[string][]string {
	rows := make(map[string][]string)
	for index := 0; index < len(entries); index += 2 {
		rows[entries[index].(string)] = entries[index+1].([]string)
	}
	return rows
}

func ids(values ...string) []string { return values }

// AuthoredInitial is the hand-written state after the seed and alice's push.
var AuthoredInitial = Checkpoint{
	Assigned: map[string][]string{
		Alice: {CatalogScope, "org:" + OrgA, "org:" + OrgB, "user:" + Alice},
		Bob:   {CatalogScope, "org:" + OrgA, "user:" + Bob},
		Chen:  {CatalogScope, "org:" + OrgB, "user:" + Chen},
		Dana:  {CatalogScope, "user:" + Dana},
	},
	Rows: ScopeRows{
		CatalogScope: scopeRows(
			"equipment", ids(Barbell, Kettlebell),
			"exercises", ids(BackSquat, BenchPress),
			"exercise_equipment", ids(SquatBarbell, BenchBarbell),
		),
		"org:" + OrgA: scopeRows(
			"organizations", ids(OrgA),
			"organization_members", ids(MemberAliceA, MemberBobA),
			"exercises", ids(OrgPress),
			"exercise_equipment", ids(PressBell),
			"programs", ids(ProgramAliceShared),
			"workouts", ids(WorkoutA1, WorkoutA2),
			"workout_exercises", ids(EntryA1Squat, EntryA1Bench, EntryA2Press),
			"exercise_sets", ids(SetA1Squat1, SetA1Squat2, SetA1Squat3, SetA1Bench1, SetA2Press1, SetA1Bench2),
		),
		"org:" + OrgB: scopeRows(
			"organizations", ids(OrgB),
			"organization_members", ids(MemberAliceB, MemberChenB),
			"programs", ids(ProgramChen),
			"workouts", ids(WorkoutC1),
			"workout_exercises", ids(EntryC1Bench),
			"exercise_sets", ids(SetC1Bench1),
		),
		"user:" + Alice: scopeRows(
			"organization_members", ids(MemberAliceA, MemberAliceB),
			"programs", ids(ProgramAliceShared, ProgramAlicePrivate),
			"workouts", ids(WorkoutA1, WorkoutA2, WorkoutA3),
			"workout_exercises", ids(EntryA1Squat, EntryA1Bench, EntryA2Press, EntryA3Squat),
			"exercise_sets", ids(SetA1Squat1, SetA1Squat2, SetA1Squat3, SetA1Bench1, SetA2Press1, SetA3Squat1, SetA1Bench2),
			"workout_media", ids(MediaAlice),
		),
		"user:" + Bob: scopeRows(
			"organization_members", ids(MemberBobA),
			"programs", ids(ProgramBob),
			"workouts", ids(WorkoutB1),
			"workout_exercises", ids(EntryB1Squat),
			"exercise_sets", ids(SetB1Squat1),
			"workout_media", ids(MediaBob),
		),
		"user:" + Chen: scopeRows(
			"organization_members", ids(MemberChenB),
			"programs", ids(ProgramChen),
			"workouts", ids(WorkoutC1),
			"workout_exercises", ids(EntryC1Bench),
			"exercise_sets", ids(SetC1Bench1),
		),
		"user:" + Dana: scopeRows(),
	},
	Values: []ValueExpectation{
		// 5*100 + 5*102.5 + 5*105.25 + 8*60 + 10*62.5
		{"workouts", WorkoutA1, "total_volume_kg", `"2643.75"`},
		{"workouts", WorkoutA1, "external_ref", `"9223372036854775807"`},
		{"workouts", WorkoutA1, "started_at", `"2026-03-02T06:30:00.123456Z"`},
		{"workouts", WorkoutA1, "scheduled_on", `"2026-03-02"`},
		{"workouts", WorkoutA2, "external_ref", `"-9223372036854775808"`},
		{"workouts", WorkoutA2, "notes", `"line one\nline \"two\"\\"`},
		{"workouts", WorkoutA2, "total_volume_kg", `"243"`},
		{"workouts", WorkoutB1, "external_ref", `"9007199254740993"`},
		{"exercises", BackSquat, "search_text", `"back squat quadriceps glutes"`},
		{"exercises", BenchPress, "search_text", `"жим лёжа chest triceps"`},
		{"exercises", BenchPress, "difficulty", `2`},
		{"exercises", OrgPress, "search_text", `"überkopfdrücken \"strict\" \\n shoulders"`},
		{"exercises", OrgPress, "metrics", `"{}"`},
		{"exercises", BackSquat, "muscle_groups", `"[\"quadriceps\",\"glutes\"]"`},
		{"exercises", BenchPress, "metrics", `"{\"bar_path\":0.125,\"unit\":\"kg\"}"`},
		{"programs", ProgramAlicePrivate, "visibility", `"private"`},
		{"programs", ProgramAlicePrivate, "week_count", `4`},
		{"organization_members", MemberBobA, "role", `"athlete"`},
		{"organization_members", MemberAliceA, "joined_at", `"2026-01-05T08:00:00.000001Z"`},
		{"exercise_sets", SetA1Squat1, "weight_kg", `"100"`},
		{"exercise_sets", SetA1Squat2, "weight_kg", `"102.5"`},
		{"exercise_sets", SetA1Squat2, "duration_ms", `"9007199254740993"`},
		{"exercise_sets", SetA1Bench1, "rpe", `1e-7`},
		{"exercise_sets", SetA1Squat1, "workout_id", `"` + WorkoutA1 + `"`},
		{"exercise_sets", SetA1Squat1, "program_id", `"` + ProgramAliceShared + `"`},
		{"exercise_sets", SetA1Bench2, "owner_id", `"alice"`},
		{"exercise_sets", SetA1Bench2, "workout_id", `"` + WorkoutA1 + `"`},
		{"exercise_sets", SetA1Bench2, "program_id", `"` + ProgramAliceShared + `"`},
		{"exercise_sets", SetA1Bench2, "note", `"新記録 🎉 \"PR\"\\"`},
		{"exercise_sets", SetA1Bench2, "duration_ms", `"-9223372036854775808"`},
		{"workout_exercises", EntryA1Squat, "program_id", `"` + ProgramAliceShared + `"`},
		{"workout_exercises", EntryA2Press, "target", `"{}"`},
		{"workout_media", MediaAlice, "byte_size", `300`},
		{"workout_media", MediaBob, "byte_size", `1024`},
	},
}

// AuthoredFinal is the hand-written state after AuthoredHistory.
var AuthoredFinal = Checkpoint{
	Assigned: map[string][]string{
		Alice: {CatalogScope, "org:" + OrgA, "org:" + OrgB, "user:" + Alice},
		Bob:   {CatalogScope, "user:" + Bob},
		Chen:  {CatalogScope, "org:" + OrgB, "user:" + Chen},
		Dana:  {CatalogScope, "org:" + OrgB, "user:" + Dana},
	},
	Rows: ScopeRows{
		CatalogScope: scopeRows(
			"equipment", ids(Barbell, Kettlebell),
			"exercises", ids(BackSquat, BenchPress),
			"exercise_equipment", ids(SquatBarbell, BenchBarbell),
		),
		"org:" + OrgA: scopeRows(
			"organizations", ids(OrgA),
			"organization_members", ids(MemberAliceA),
			"exercises", ids(OrgPress),
			"exercise_equipment", ids(PressBell),
			"programs", ids(ProgramAliceShared),
			"workouts", ids(WorkoutA1, WorkoutA2),
			"workout_exercises", ids(EntryA1Squat, EntryA1Bench, EntryA2Press),
			"exercise_sets", ids(SetA1Squat1, SetA1Squat2, SetA1Squat3, SetA1Bench1, SetA2Press1, SetA1Bench2),
		),
		"org:" + OrgB: scopeRows(
			"organizations", ids(OrgB),
			"organization_members", ids(MemberAliceB, MemberChenB, MemberDanaB),
			"programs", ids(ProgramAlicePrivate, ProgramChen),
			"workouts", ids(WorkoutA3, WorkoutC1),
			"workout_exercises", ids(EntryA3Squat, EntryC1Bench),
			"exercise_sets", ids(SetA3Squat1, SetC1Bench1),
		),
		"user:" + Alice: scopeRows(
			"organization_members", ids(MemberAliceA, MemberAliceB),
			"programs", ids(ProgramAliceShared, ProgramAlicePrivate),
			"workouts", ids(WorkoutA1, WorkoutA2, WorkoutA3),
			"workout_exercises", ids(EntryA1Squat, EntryA1Bench, EntryA2Press, EntryA3Squat),
			"exercise_sets", ids(SetA1Squat1, SetA1Squat2, SetA1Squat3, SetA1Bench1, SetA2Press1, SetA3Squat1, SetA1Bench2),
			"workout_media", ids(MediaAlice),
		),
		"user:" + Bob: scopeRows(
			"workout_media", ids(MediaBob),
		),
		"user:" + Chen: scopeRows(
			"organization_members", ids(MemberChenB),
			"programs", ids(ProgramChen),
			"workouts", ids(WorkoutC1),
			"workout_exercises", ids(EntryC1Bench),
			"exercise_sets", ids(SetC1Bench1),
		),
		"user:" + Dana: scopeRows(
			"organization_members", ids(MemberDanaB),
		),
	},
	Values: []ValueExpectation{
		// 5*102.5 + 3*112.5 + 5*107.75 + 8*60 + 10*62.5
		{"workouts", WorkoutA1, "total_volume_kg", `"2493.75"`},
		{"exercise_sets", SetA1Squat1, "weight_kg", `"102.5"`},
		{"exercise_sets", SetA1Squat2, "reps", `3`},
		{"exercise_sets", SetA1Squat2, "weight_kg", `"112.5"`},
		{"exercise_sets", SetA1Squat2, "note", `"re-created"`},
		{"exercise_sets", SetA1Squat2, "duration_ms", `null`},
		{"exercise_sets", SetA1Squat3, "weight_kg", `"107.75"`},
		{"exercises", BackSquat, "name", `"Back Squat (High Bar)"`},
		{"exercises", BackSquat, "search_text", `"back squat (high bar) quadriceps glutes"`},
		{"programs", ProgramAlicePrivate, "visibility", `"organization"`},
		{"organization_members", MemberDanaB, "role", `"athlete"`},
	},
}
