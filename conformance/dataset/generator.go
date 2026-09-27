package dataset

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Size selects one bounded scaled workload.
type Size struct {
	Name              string `json:"name"`
	Organizations     int    `json:"organizations"`
	Users             int    `json:"users"`
	MaxPrograms       int    `json:"max_programs_per_user"`
	MaxWorkouts       int    `json:"max_workouts_per_program"`
	CatalogExercises  int    `json:"catalog_exercises"`
	HistoryOperations int    `json:"history_operations"`
}

// Sizes are the supported bounded workloads, from smallest to largest.
var Sizes = []Size{
	{Name: "s", Organizations: 3, Users: 12, MaxPrograms: 3, MaxWorkouts: 4, CatalogExercises: 40, HistoryOperations: 40},
	{Name: "m", Organizations: 8, Users: 60, MaxPrograms: 5, MaxWorkouts: 8, CatalogExercises: 150, HistoryOperations: 200},
	{Name: "l", Organizations: 16, Users: 200, MaxPrograms: 6, MaxWorkouts: 10, CatalogExercises: 400, HistoryOperations: 600},
}

// LookupSize returns one supported size by name.
func LookupSize(name string) (Size, bool) {
	for _, size := range Sizes {
		if size.Name == name {
			return size, true
		}
	}
	return Size{}, false
}

// MaxTransactionRecords bounds the estimated pgoutput records of one generated
// source transaction. It keeps generated work well inside the fixed decoder
// limit of 10,000 records and 16 MiB. Each registered row change produces one
// row record and one fence message.
const MaxTransactionRecords = 4000

// Statement is one parameterized source SQL statement.
type Statement struct {
	SQL  string
	Args []any
}

// Transaction is one bounded source transaction or one operator action.
type Transaction struct {
	Kind       string      `json:"kind"`
	Statements []Statement `json:"-"`
	Grants     [][2]string `json:"grants,omitempty"`
	Revokes    [][2]string `json:"revokes,omitempty"`
	Records    int         `json:"estimated_records"`
	Bytes      int         `json:"estimated_bytes"`
}

// Stats records the generated distributions for characterization reports.
type Stats struct {
	Rows              map[string]int    `json:"rows"`
	TextBytes         map[string][3]int `json:"text_bytes_p50_p90_max"`
	UsersPerOrg       []int             `json:"users_per_organization"`
	ProgramsPerUser   []int             `json:"programs_per_user"`
	UsersInManyOrgs   int               `json:"users_in_more_than_one_organization"`
	SharedPrograms    int               `json:"organization_visible_programs"`
	Scopes            int               `json:"scopes"`
	HistoryMix        map[string]int    `json:"history_operation_mix"`
	MaxRecords        int               `json:"max_transaction_estimated_records"`
	InitialRecords    int               `json:"initial_estimated_records"`
	RepeatedKeyWrites int               `json:"repeated_key_writes"`
}

// Plan is the complete deterministic workload for one seed and size.
type Plan struct {
	Seed    uint64        `json:"seed"`
	Size    Size          `json:"size"`
	Users   []string      `json:"users"`
	Initial []Transaction `json:"-"`
	History []Transaction `json:"-"`
	Stats   Stats         `json:"stats"`
	// SetsByUser lists the exercise sets that each user owns after Initial.
	SetsByUser map[string][]string `json:"-"`
}

// ErrInvalidSize reports an unsupported size.
var ErrInvalidSize = errors.New("dataset size is invalid")

type organization struct {
	id      string
	members []string
}

type program struct {
	id, org, owner string
	shared, dead   bool
}

type set struct {
	id, entry, owner string
	dead             bool
}

type generator struct {
	random   *Random
	size     Size
	plan     Plan
	orgs     []organization
	members  map[string]map[int]string
	programs []*program
	sets     []*set
	workouts []string
	catalog  []string
	pending  Transaction
	widths   map[string][]int
	parents  map[string]*program
}

// Generate returns the deterministic plan for one seed and supported size.
func Generate(seed uint64, size Size) (Plan, error) {
	if !slices.Contains(Sizes, size) {
		return Plan{}, ErrInvalidSize
	}
	g := &generator{
		random:  NewRandom(seed),
		size:    size,
		members: make(map[string]map[int]string),
		widths:  make(map[string][]int),
		parents: make(map[string]*program),
		plan: Plan{Seed: seed, Size: size, Stats: Stats{
			Rows: make(map[string]int), TextBytes: make(map[string][3]int), HistoryMix: make(map[string]int),
		}},
	}
	g.catalogRows()
	g.organizations()
	for index := 0; index < size.Users; index++ {
		g.userRows(fmt.Sprintf("athlete-%04d", index+1))
	}
	g.flush()
	for _, transaction := range g.plan.Initial {
		g.plan.Stats.InitialRecords += transaction.Records
	}
	g.plan.SetsByUser = make(map[string][]string)
	for _, current := range g.sets {
		g.plan.SetsByUser[current.owner] = append(g.plan.SetsByUser[current.owner], current.id)
	}
	for index := 0; index < size.HistoryOperations; index++ {
		g.historyOperation()
	}
	g.finishStats()
	return g.plan, nil
}

// add appends one statement to the pending transaction. rows is the number
// of registered row changes, including trigger effects.
func (g *generator) add(kind string, rows, bytes int, sql string, args ...any) {
	records := 2 * rows
	if g.pending.Kind != "" && (g.pending.Kind != kind || g.pending.Records+records > MaxTransactionRecords) {
		g.flush()
	}
	g.pending.Kind = kind
	g.pending.Statements = append(g.pending.Statements, Statement{SQL: sql, Args: args})
	g.pending.Records += records
	g.pending.Bytes += bytes
}

func (g *generator) flush() {
	if g.pending.Kind == "" {
		return
	}
	g.plan.Stats.MaxRecords = max(g.plan.Stats.MaxRecords, g.pending.Records)
	g.plan.Initial = append(g.plan.Initial, g.pending)
	g.pending = Transaction{}
}

func (g *generator) text(column string, value string) string {
	g.widths[column] = append(g.widths[column], len(value))
	return value
}

func (g *generator) row(table string) {
	g.plan.Stats.Rows[table]++
}

func (g *generator) catalogRows() {
	equipment := make([]string, 0, 24)
	for index := 0; index < 24; index++ {
		id := g.random.UUID()
		equipment = append(equipment, id)
		name := g.text("equipment.name", g.random.Phrase(1, 3))
		g.row("equipment")
		g.add("catalog", 1, len(name), "INSERT INTO equipment (id, name, category) VALUES ($1, $2, $3)",
			id, name, categories[index%len(categories)])
	}
	for index := 0; index < g.size.CatalogExercises; index++ {
		id := g.exercise(nil)
		g.catalog = append(g.catalog, id)
		g.links(id, equipment)
	}
}

func (g *generator) exercise(org *string) string {
	id := g.random.UUID()
	name := g.text("exercises.name", g.random.Phrase(1, 4))
	instructions := g.text("exercises.instructions", g.random.Paragraph(0, 1200))
	muscles := g.random.Muscles()
	metrics := fmt.Sprintf(`{"unit":"kg","tracks":["reps","weight"],"version":%d}`, g.random.IntN(5))
	g.row("exercises")
	var orgID any
	if org != nil {
		orgID = *org
	}
	g.add("catalog", 1, len(name)+len(instructions)+len(metrics),
		"INSERT INTO exercises (id, organization_id, name, instructions, muscle_groups, difficulty, metrics) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		id, orgID, name, instructions, muscles, 1+g.random.IntN(5), metrics)
	return id
}

func (g *generator) links(exercise string, equipment []string) {
	count := 1 + g.random.IntN(3)
	chosen := make(map[int]bool)
	for len(chosen) < count {
		chosen[g.random.IntN(len(equipment))] = true
	}
	for index := range len(equipment) {
		if !chosen[index] {
			continue
		}
		g.row("exercise_equipment")
		g.add("catalog", 1, 0, "INSERT INTO exercise_equipment (id, exercise_id, equipment_id) VALUES ($1, $2, $3)",
			g.random.UUID(), exercise, equipment[index])
	}
}

func (g *generator) organizations() {
	for index := 0; index < g.size.Organizations; index++ {
		id := g.random.UUID()
		name := g.text("organizations.name", g.random.Phrase(2, 4))
		g.row("organizations")
		g.add("organizations", 1, len(name), "INSERT INTO organizations (id, name, slug, settings) VALUES ($1, $2, $3, $4)",
			id, name, fmt.Sprintf("org-%03d", index+1), fmt.Sprintf(`{"units":"%s"}`, []string{"kg", "lb"}[index%2]))
		g.orgs = append(g.orgs, organization{id: id})
		for count := 0; count < 3; count++ {
			org := id
			g.exercise(&org)
		}
	}
}

// memberships picks one to three organizations with a skewed preference for
// low organization numbers.
func (g *generator) memberships(user string) []int {
	count := 1
	if roll := g.random.IntN(100); roll >= 70 {
		count = 2 + g.random.IntN(2)
	}
	chosen := make([]int, 0, count)
	for len(chosen) < min(count, len(g.orgs)) {
		index := g.random.Skewed(len(g.orgs))
		if !slices.Contains(chosen, index) {
			chosen = append(chosen, index)
		}
	}
	slices.Sort(chosen)
	g.members[user] = make(map[int]string)
	for _, index := range chosen {
		member := g.random.UUID()
		g.members[user][index] = member
		g.orgs[index].members = append(g.orgs[index].members, user)
		g.row("organization_members")
		g.add("organizations", 1, 0,
			"INSERT INTO organization_members (id, organization_id, user_id, role) VALUES ($1, $2, $3, $4)",
			member, g.orgs[index].id, user, []string{"athlete", "athlete", "coach"}[g.random.IntN(3)])
	}
	return chosen
}

func (g *generator) userRows(user string) {
	g.plan.Users = append(g.plan.Users, user)
	orgs := g.memberships(user)
	grants := make([][2]string, 0, len(orgs))
	for _, index := range orgs {
		grants = append(grants, [2]string{user, "org:" + g.orgs[index].id})
	}
	g.flush()
	g.plan.Initial = append(g.plan.Initial, Transaction{Kind: "grant", Grants: grants})

	programs := 1 + g.random.Skewed(g.size.MaxPrograms)
	g.plan.Stats.ProgramsPerUser = append(g.plan.Stats.ProgramsPerUser, programs)
	for count := 0; count < programs; count++ {
		g.program(user, g.orgs[orgs[g.random.IntN(len(orgs))]].id)
	}
	g.flush()
}

func (g *generator) program(user, org string) {
	current := &program{id: g.random.UUID(), org: org, owner: user, shared: g.random.IntN(100) < 35}
	g.programs = append(g.programs, current)
	visibility := "private"
	if current.shared {
		visibility = "organization"
	}
	title := g.text("programs.title", g.random.Phrase(2, 5))
	description := g.text("programs.description", g.random.Paragraph(0, 2000))
	g.row("programs")
	g.add("user", 1, len(title)+len(description),
		"INSERT INTO programs (id, organization_id, owner_id, title, description, visibility, week_count, settings) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		current.id, org, user, title, description, visibility, 1+g.random.IntN(16), fmt.Sprintf(`{"deload_week":%d}`, 1+g.random.IntN(6)))

	workouts := 1 + g.random.Skewed(g.size.MaxWorkouts)
	for index := 0; index < workouts; index++ {
		workout := g.random.UUID()
		g.workouts = append(g.workouts, workout)
		name := g.text("workouts.name", g.random.Phrase(1, 3))
		notes := g.text("workouts.notes", g.random.Paragraph(0, 600))
		g.row("workouts")
		// A workout insert also touches its program.
		g.add("user", 2, len(name)+len(notes),
			"INSERT INTO workouts (id, program_id, owner_id, name, notes, scheduled_on, started_at, duration_seconds, external_ref) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
			workout, current.id, user, name, notes, g.random.Date(), g.random.OptionalTimestamp(), g.random.OptionalInt(7200), g.random.Int64())
		entries := 2 + g.random.IntN(5)
		for position := 1; position <= entries; position++ {
			entry := g.random.UUID()
			g.parents[entry] = current
			g.row("workout_exercises")
			g.add("user", 1, 32,
				"INSERT INTO workout_exercises (id, workout_id, exercise_id, owner_id, position, target) VALUES ($1, $2, $3, $4, $5, $6)",
				entry, workout, g.catalog[g.random.Skewed(len(g.catalog))], user, position,
				fmt.Sprintf(`{"sets":%d,"reps":%d}`, 1+g.random.IntN(5), 1+g.random.IntN(12)))
			sets := 1 + g.random.IntN(5)
			for index := 1; index <= sets; index++ {
				g.newSet(user, entry, index)
			}
		}
		if g.random.IntN(100) < 10 {
			body := g.random.Bytes(64 + g.random.IntN(8129))
			g.row("workout_media")
			g.add("user", 1, len(body),
				"INSERT INTO workout_media (id, workout_id, owner_id, content_type, body) VALUES ($1, $2, $3, $4, $5)",
				g.random.UUID(), workout, user, "image/jpeg", body)
		}
	}
}

func (g *generator) newSet(user, entry string, index int) {
	current := &set{id: g.random.UUID(), entry: entry, owner: user}
	g.sets = append(g.sets, current)
	note := g.text("exercise_sets.note", g.random.Paragraph(0, 120))
	g.row("exercise_sets")
	// A set insert also updates its workout, which touches its program.
	g.add("user", 3, len(note),
		"INSERT INTO exercise_sets (id, workout_exercise_id, owner_id, set_index, reps, weight_kg, rpe, duration_ms, completed_at, note) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		current.id, entry, user, index, g.random.IntN(20), g.random.Weight(), g.random.RPE(), g.random.OptionalInt64(), g.random.OptionalTimestamp(), note)
}

var historyKinds = []string{
	"update-set", "update-set", "update-set", "bulk-update-sets", "soft-delete-set",
	"hard-delete-re-create-set", "flip-program-visibility", "soft-delete-program",
	"rename-catalog-exercise", "join-organization", "leave-organization", "repeat-key-updates",
}

func (g *generator) liveSet() *set {
	for attempt := 0; attempt < 64; attempt++ {
		candidate := g.sets[g.random.IntN(len(g.sets))]
		if !candidate.dead && !g.programOf(candidate.entry).dead {
			return candidate
		}
	}
	return nil
}

func (g *generator) programOf(entry string) *program {
	return g.parents[entry]
}

func (g *generator) history(kind string, rows int, statements []Statement, grants, revokes [][2]string) {
	records := 2 * rows
	g.plan.History = append(g.plan.History, Transaction{
		Kind: kind, Statements: statements, Grants: grants, Revokes: revokes, Records: records,
	})
	g.plan.Stats.HistoryMix[kind]++
	g.plan.Stats.MaxRecords = max(g.plan.Stats.MaxRecords, records)
}

func (g *generator) historyOperation() {
	kind := historyKinds[g.random.IntN(len(historyKinds))]
	switch kind {
	case "update-set":
		if current := g.liveSet(); current != nil {
			g.history(kind, 3, []Statement{{
				SQL:  "UPDATE exercise_sets SET reps = $2, weight_kg = $3, note = $4, updated_at = clock_timestamp() WHERE id = $1",
				Args: []any{current.id, g.random.IntN(20), g.random.Weight(), g.random.Paragraph(0, 80)},
			}}, nil, nil)
		}
	case "bulk-update-sets":
		if current := g.liveSet(); current != nil {
			count := 0
			for _, other := range g.sets {
				if other.entry == current.entry {
					count++
				}
			}
			g.history(kind, 3*count, []Statement{{
				SQL:  "UPDATE exercise_sets SET weight_kg = weight_kg + 2.5, updated_at = clock_timestamp() WHERE workout_exercise_id = $1 AND deleted_at IS NULL",
				Args: []any{current.entry},
			}}, nil, nil)
		}
	case "soft-delete-set":
		if current := g.liveSet(); current != nil {
			current.dead = true
			g.history(kind, 3, []Statement{{
				SQL:  "UPDATE exercise_sets SET deleted_at = clock_timestamp(), updated_at = clock_timestamp() WHERE id = $1",
				Args: []any{current.id},
			}}, nil, nil)
		}
	case "hard-delete-re-create-set":
		if current := g.liveSet(); current != nil {
			current.dead = true
			g.history("hard-delete-set", 3, []Statement{{SQL: "DELETE FROM exercise_sets WHERE id = $1", Args: []any{current.id}}}, nil, nil)
			// A deleted row identity is permanent. The re-created set gets a new one.
			replacement := &set{id: g.random.UUID(), entry: current.entry, owner: current.owner}
			g.sets = append(g.sets, replacement)
			g.history("re-create-set", 3, []Statement{{
				SQL:  "INSERT INTO exercise_sets (id, workout_exercise_id, owner_id, set_index, reps, weight_kg, note) VALUES ($1, $2, $3, 1, $4, $5, 're-created')",
				Args: []any{replacement.id, current.entry, current.owner, g.random.IntN(20), g.random.Weight()},
			}}, nil, nil)
		}
	case "flip-program-visibility":
		current := g.programs[g.random.IntN(len(g.programs))]
		if !current.dead {
			current.shared = !current.shared
			visibility := map[bool]string{true: "organization", false: "private"}[current.shared]
			g.history(kind, 1, []Statement{{
				SQL:  "UPDATE programs SET visibility = $2, updated_at = clock_timestamp() WHERE id = $1",
				Args: []any{current.id, visibility},
			}}, nil, nil)
		}
	case "soft-delete-program":
		current := g.programs[g.random.IntN(len(g.programs))]
		if !current.dead {
			current.dead = true
			g.history(kind, 1, []Statement{{
				SQL:  "UPDATE programs SET deleted_at = clock_timestamp(), updated_at = clock_timestamp() WHERE id = $1",
				Args: []any{current.id},
			}}, nil, nil)
		}
	case "rename-catalog-exercise":
		g.history(kind, 1, []Statement{{
			SQL:  "UPDATE exercises SET name = $2, updated_at = clock_timestamp() WHERE id = $1",
			Args: []any{g.catalog[g.random.Skewed(len(g.catalog))], g.random.Phrase(1, 4)},
		}}, nil, nil)
	case "join-organization":
		user := g.plan.Users[g.random.IntN(len(g.plan.Users))]
		index := g.random.IntN(len(g.orgs))
		if _, member := g.members[user][index]; !member {
			id := g.random.UUID()
			g.members[user][index] = id
			g.history(kind, 1, []Statement{{
				SQL:  "INSERT INTO organization_members (id, organization_id, user_id) VALUES ($1, $2, $3)",
				Args: []any{id, g.orgs[index].id, user},
			}}, [][2]string{{user, "org:" + g.orgs[index].id}}, nil)
		}
	case "leave-organization":
		user := g.plan.Users[g.random.IntN(len(g.plan.Users))]
		indexes := make([]int, 0, len(g.members[user]))
		for index := range g.members[user] {
			indexes = append(indexes, index)
		}
		slices.Sort(indexes)
		for _, index := range indexes {
			member := g.members[user][index]
			// A user keeps the organizations of their own programs.
			owns := slices.ContainsFunc(g.programs, func(current *program) bool {
				return current.owner == user && current.org == g.orgs[index].id
			})
			if owns {
				continue
			}
			delete(g.members[user], index)
			g.history(kind, 1, []Statement{{
				SQL:  "UPDATE organization_members SET deleted_at = clock_timestamp(), updated_at = clock_timestamp() WHERE id = $1",
				Args: []any{member},
			}}, nil, [][2]string{{user, "org:" + g.orgs[index].id}})
			break
		}
	case "repeat-key-updates":
		if current := g.liveSet(); current != nil {
			count := 50 + g.random.IntN(150)
			statements := make([]Statement, 0, count)
			for index := 0; index < count; index++ {
				statements = append(statements, Statement{
					SQL:  "UPDATE exercise_sets SET reps = $2, updated_at = clock_timestamp() WHERE id = $1",
					Args: []any{current.id, index % 20},
				})
			}
			g.plan.Stats.RepeatedKeyWrites += count
			g.history(kind, 3*count, statements, nil, nil)
		}
	}
}

func (g *generator) finishStats() {
	stats := &g.plan.Stats
	shared := 0
	for _, current := range g.programs {
		if current.shared && !current.dead {
			shared++
		}
	}
	stats.SharedPrograms = shared
	scopes := map[string]bool{CatalogScope: true}
	for _, org := range g.orgs {
		stats.UsersPerOrg = append(stats.UsersPerOrg, len(org.members))
		scopes["org:"+org.id] = true
	}
	for _, user := range g.plan.Users {
		scopes["user:"+user] = true
		if len(g.members[user]) > 1 {
			stats.UsersInManyOrgs++
		}
	}
	stats.Scopes = len(scopes)
	for column, widths := range g.widths {
		slices.Sort(widths)
		stats.TextBytes[column] = [3]int{widths[len(widths)/2], widths[len(widths)*9/10], widths[len(widths)-1]}
	}
}

var categories = []string{"free_weight", "machine", "bodyweight", "cardio", "mobility"}

var words = []string{
	"squat", "press", "row", "deadlift", "lunge", "carry", "plank", "sprint", "tempo", "pause",
	"Kniebeuge", "Überzug", "Жим", "тяга", "スクワット", "ベンチ", "深蹲", "卧推", "sentadilla", "remo",
	"🏋️", "💪", "\"strict\"", "back\\slash", "O'Brien", "line\nbreak", "tab\tstop", "ÅÄÖ", "ñandú", "élan",
}

var muscles = []string{"quadriceps", "glutes", "hamstrings", "chest", "triceps", "shoulders", "lats", "core", "calves"}

func muscleArray(values []string) string {
	return "{" + strings.Join(values, ",") + "}"
}
