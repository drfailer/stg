package profiler

import "core:fmt"
import "core:time"
import "core:sync"
import "core:container/small_array"

PROFILER_ENABLED :: #config(PROFILER_ENABLED, true)
MAX_ENTRY_STACK :: #config(MAX_ENTRY_STACK, 64)

GLOBAL_PROFILERS := Profilers{}

Profilers :: struct {
    profilers: map[int]^Profiler,
    global_entries: map[string]GlobalProfileEntry,
    mutex: sync.Mutex,
    stopwatch: time.Stopwatch,
    started: bool,
}

Profiler :: struct {
    entries: map[string]ProfileEntry,
    entry_stack: small_array.Small_Array(MAX_ENTRY_STACK, string),
    entry_stack_overflow_counter: uint,
}

ProfileEntry :: struct {
    parents: map[string]ParentProfileInfo,
    stopwatch: time.Stopwatch,
    min: time.Duration,
    max: time.Duration,
    ttl: time.Duration,
    count: int,
}

ParentProfileInfo :: struct {
    call_count: uint,
}

GlobalProfileEntry :: struct {
    using entry: ProfileEntry,
    thread_count: uint,
}

// init ////////////////////////////////////////////////////////////////////////

init :: proc() {
    GLOBAL_PROFILERS.profilers = make(map[int]^Profiler)
    GLOBAL_PROFILERS.global_entries = make(map[string]GlobalProfileEntry)
}

fini :: proc() {
    time.stopwatch_stop(&GLOBAL_PROFILERS.stopwatch)
    for _, &profiler in GLOBAL_PROFILERS.profilers {
        for _, &entry in profiler.entries {
            delete(entry.parents)
        }
        delete(profiler.entries)
        free(profiler)
    }
    delete(GLOBAL_PROFILERS.profilers)
    for _, &entry in GLOBAL_PROFILERS.global_entries {
        delete(entry.parents)
    }
    delete(GLOBAL_PROFILERS.global_entries)
}

start :: proc() {
    time.stopwatch_start(&GLOBAL_PROFILERS.stopwatch)
    GLOBAL_PROFILERS.started = true
}

stop :: proc() {
    time.stopwatch_stop(&GLOBAL_PROFILERS.stopwatch)
    GLOBAL_PROFILERS.started = false

    // compute the global entries (merge informations from all the threads)
    for thread_id, profiler in GLOBAL_PROFILERS.profilers {
        for entry_name, entry in profiler.entries {
            global_entry := map_get_ptr(&GLOBAL_PROFILERS.global_entries, entry_name)
            for parent_name, parent_info in entry.parents {
                parent_info := map_get_ptr(&global_entry.parents, parent_name)
                parent_info.call_count += parent_info.call_count
            }
            global_entry.min = min(entry.min, global_entry.min) if global_entry.min > 0 else entry.min
            global_entry.max = max(entry.max, global_entry.max)
            global_entry.ttl += entry.ttl
            global_entry.count += entry.count
            global_entry.thread_count += 1
        }
    }
}


//
// This allows threads to register to the profiler. It is important to register
// the threads before the profiling starts as the profile functions don't use
// the profiler mutex to limit overhead (they expect the map not to be changed
// while profiling).
//
register :: proc() {
    sync.lock(&GLOBAL_PROFILERS.mutex)
    defer sync.unlock(&GLOBAL_PROFILERS.mutex)

    // assert(GLOBAL_PROFILERS.started == false)
    thread_id := sync.current_thread_id()
    ensure(thread_id not_in GLOBAL_PROFILERS.profilers, "cannot register the same thread twice")
    profiler := new(Profiler)
    profiler.entries = make(map[string]ProfileEntry)
    small_array.push(&profiler.entry_stack, "src")
    GLOBAL_PROFILERS.profilers[thread_id] = profiler

}

// region //////////////////////////////////////////////////////////////////////

//
// profile a specific region of the code
//

region_begin :: proc(name: string) {
    profiler := get_profiler()

    // get or insert the entry
    entry, found := &profiler.entries[name]
    if !found {
        profiler.entries[name] = ProfileEntry{}
        entry = &profiler.entries[name]
    }

    // update the parent info
    entry_stack := small_array.slice(&profiler.entry_stack)
    parent_name := entry_stack[len(entry_stack) - 1]
    parent_info, parent_found := &entry.parents[parent_name]
    if !parent_found {
        entry.parents[parent_name] = ParentProfileInfo{}
        parent_info = &entry.parents[parent_name]
    }
    parent_info.call_count += 1

    // update the call stack if possible
    if !small_array.push_back(&profiler.entry_stack, name) do profiler.entry_stack_overflow_counter += 1

    // start the region timer
    time.stopwatch_reset(&entry.stopwatch)
    time.stopwatch_start(&entry.stopwatch)
}

region_end :: proc(name: string) {
    profiler := get_profiler()
    entry, found := &profiler.entries[name]
    assert(found)

    // stop the region timer and get the duration
    time.stopwatch_stop(&entry.stopwatch)
    duration := time.stopwatch_duration(entry.stopwatch)

    // profile info update
    entry.min = min(entry.min, duration) if entry.min > 0 else duration
    entry.max = max(entry.max, duration)
    entry.ttl += duration
    entry.count += 1

    // call stack update
    if profiler.entry_stack_overflow_counter > 0 do profiler.entry_stack_overflow_counter -= 1
    if profiler.entry_stack_overflow_counter == 0 do small_array.pop_back(&profiler.entry_stack)
}

@(deferred_in=region_end)
region :: proc(name: string) -> bool {
    region_begin(name)
    return true
}

procedure_end :: proc(loc := #caller_location) {
    region_end(loc.procedure)
}

@(deferred_in=procedure_end)
procedure :: proc(loc := #caller_location) {
    region_begin(loc.procedure)
}

// report //////////////////////////////////////////////////////////////////////

ReportFormat :: enum {
    Text,
    Dot,
    // html table?
    // json?
}

generate_report :: proc(filename: string, format := ReportFormat.Text) {
    panic("unimplemented")
}

// should only be called by the main thread
print_report :: proc() {
    global_ttl := time.stopwatch_duration(GLOBAL_PROFILERS.stopwatch)

    for thread_id, profiler in GLOBAL_PROFILERS.profilers {
        fmt.printfln("Profile of thread `{}` (profiler = {}):", thread_id, uintptr(profiler))
        for entry_name, entry in profiler.entries {
            str_ttl := duration_to_string(entry.ttl, context.temp_allocator)
            avg := time.Duration(f64(entry.ttl) / f64(entry.count))
            str_avg := duration_to_string(avg, context.temp_allocator)
            str_min := duration_to_string(entry.min, context.temp_allocator)
            str_max := duration_to_string(entry.max, context.temp_allocator)
            percent := 100 * (f64(entry.ttl) / f64(global_ttl))
            fmt.printfln("- {}: avg = {} [{}-{}] / ttl = {}, count = {} ({}%%)",
                entry_name, str_avg, str_min, str_max, str_ttl, entry.count, percent)

            // print parent infos
            for parent_name, parent_info in entry.parents {
                parent_entry, parent_found := &profiler.entries[parent_name]
                parent_ttl := parent_entry.ttl if parent_found else time.stopwatch_duration(GLOBAL_PROFILERS.stopwatch)
                parent_str_ttl := duration_to_string(parent_ttl, context.temp_allocator)
                percent := 100 * (f64(entry.ttl) / f64(parent_ttl))
                fmt.printfln("  - parent {}(calls = {}): child ttl = {} / parent ttl = {} ({}%%)",
                    parent_name, parent_info.call_count, str_ttl, parent_str_ttl, percent)
            }

            // free the allocated strings
            free_all(context.temp_allocator)
        }
    }

    fmt.println("Global entries accross all threads:")
    for entry_name, entry in GLOBAL_PROFILERS.global_entries {
        avg := time.Duration(f64(entry.ttl) / f64(entry.count))
        str_avg := duration_to_string(avg, context.temp_allocator)
        str_min := duration_to_string(entry.min, context.temp_allocator)
        str_max := duration_to_string(entry.max, context.temp_allocator)
        str_ttl := duration_to_string(entry.ttl, context.temp_allocator)
        ttl_avg := f64(entry.ttl) / f64(entry.thread_count)
        percent := 100 * (ttl_avg / f64(global_ttl))
        fmt.printfln("- {}: avg = {} [{}-{}] / ttl = {}, count = {}, threads = {}, ({}%%)",
            entry_name, str_avg, str_min, str_max, str_ttl, entry.count, entry.thread_count, percent)

        for parent_name, parent_info in entry.parents {
            parent_entry, parent_found := &GLOBAL_PROFILERS.global_entries[parent_name]
            parent_ttl := parent_entry.ttl if parent_found else time.stopwatch_duration(GLOBAL_PROFILERS.stopwatch)
            parent_str_ttl := duration_to_string(parent_ttl, context.temp_allocator)
            percent := 100 * (f64(entry.ttl) / f64(parent_ttl))
            fmt.printfln("  - parent {}(calls = {}): child ttl = {} / parent ttl = {} ({}%%)", parent_name, parent_info.call_count,
                str_ttl, parent_str_ttl, percent)
        }
    }
    ttl_time_str := duration_to_string(global_ttl)
    defer delete(ttl_time_str)
    fmt.printfln("Profiler total time: {}", ttl_time_str)
}

duration_to_string :: proc(dur: time.Duration, allocator := context.allocator) -> string {
    ns := time.duration_nanoseconds(dur)
    if ns < 0 { ns = 0 }

    s := ns / 1_000_000_000
    ms := ns / 1_000_000
    us := ns / 1_000

    if s > 0 {
        remainder_ms := (ns - s * 1_000_000_000) / 1_000_000
        return fmt.aprintf("%d.%03ds", s, remainder_ms, allocator = allocator)
    } else if ms > 0 {
        remainder_us := (ns - ms * 1_000_000) / 1_000
        return fmt.aprintf("%d.%03dms", ms, remainder_us, allocator = allocator)
    } else if us > 0 {
        remainder_ns := ns - us * 1_000
        return fmt.aprintf("%d.%03dus", us, remainder_ns, allocator = allocator)
    } else {
        return fmt.aprintf("%dns", ns, allocator = allocator)
    }
}

// internals ///////////////////////////////////////////////////////////////////

@(private="file")
get_profiler :: proc() -> ^Profiler {
    // we shouldn't have to lock ere if the profilers are registered before the
    // threads start profiling
    // sync.lock(&GLOBAL_PROFILERS.mutex)
    // defer sync.unlock(&GLOBAL_PROFILERS.mutex)

    thread_id := sync.current_thread_id()
    profiler, ok := GLOBAL_PROFILERS.profilers[thread_id]
    ensure(ok, "the current thread is not registered in the profiler")
    return profiler
}

@(private="file")
map_get_ptr :: proc(m: ^map[$K]$V, key: K) -> ^V {
    value_ptr, found := &m[key]
    if !found {
        m[key] = {}
        value_ptr = &m[key]
    }
    return value_ptr
}
