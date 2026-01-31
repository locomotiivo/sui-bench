/*
/// Module: tps_test
module tps_test::tps_test;
*/

// For Move coding conventions, see
// https://docs.sui.io/concepts/sui-move-concepts/conventions

module tps_test::tps_test {
    use sui::table::{Self, Table};
    use sui::event;

    // 1. Core counter object
    public struct Counter has key, store {
        id: UID,
        value: u64
    }

    // 2. Global state
    public struct GlobalState has key {
        id: UID,
        total_created: u64,
        registry: Table<u64, ID>
    }

    // Event
    public struct NewCounterEvent has copy, drop {
        key: u64,
        object_id: ID
    }

    // Initialization
    fun init(ctx: &mut TxContext) {
        transfer::share_object(GlobalState {
            id: object::new(ctx),
            total_created: 0,
            registry: table::new(ctx)
        });
    }

    // =========================================================
    // Phase One: Setup
    // =========================================================
    public fun create_counter(state: &mut GlobalState, ctx: &mut TxContext) {
        let key = state.total_created;
        
        let counter = Counter {
            id: object::new(ctx),
            value: 0
        };
        
        let counter_id = object::id(&counter);

        table::add(&mut state.registry, key, counter_id);
        state.total_created = key + 1;

        event::emit(NewCounterEvent {
            key,
            object_id: counter_id
        });

        transfer::share_object(counter);
    }

    // =========================================================
    // Phase Two: High-Frequency Testing (TPS Run)
    // =========================================================
    public fun operate(counter: &mut Counter, _ctx: &mut TxContext) {
        counter.value = counter.value + 1;
    }

    // =========================================================
    // Query Interface
    // =========================================================

    public fun get_total_created(state: &GlobalState): u64 {
        state.total_created
    }

    public fun get_counter_id(state: &GlobalState, key: u64): ID {
        *table::borrow(&state.registry, key)
    }

    public fun get_value(counter: &Counter): u64 {
        counter.value
    }

    #[test_only]
    public fun init_for_testing(ctx: &mut TxContext) {
        init(ctx);
    }
}