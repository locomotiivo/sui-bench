/*
#[test_only]
module tps_test::tps_test_tests;
// uncomment this line to import the module
// use tps_test::tps_test;

const ENotImplemented: u64 = 0;

#[test]
fun test_tps_test() {
    // pass
}

#[test, expected_failure(abort_code = ::tps_test::tps_test_tests::ENotImplemented)]
fun test_tps_test_fail() {
    abort ENotImplemented
}
*/

#[test_only]
module tps_test::tests {
    use sui::test_scenario::{Self as ts};
    // [REMOVED] use sui::object::{Self}; // This line caused warnings, not needed in Move 2024
    use tps_test::tps_test::{Self, GlobalState, Counter};

    // Define test addresses
    const ADMIN: address = @0xA;
    const USER: address = @0xB;

    #[test]
    fun test_tps_workflow() {
        // 1. Start test scenario
        let mut scenario = ts::begin(ADMIN);

        // =========================================================
        // Step 1: Initialization (Init)
        // =========================================================
        {
            tps_test::init_for_testing(ts::ctx(&mut scenario));
        };

        // =========================================================
        // Step 2: Setup Phase - Create Counters
        // =========================================================
        ts::next_tx(&mut scenario, ADMIN);
        {
            let mut state = ts::take_shared<GlobalState>(&scenario);

            // Create Key = 0
            tps_test::create_counter(&mut state, ts::ctx(&mut scenario));
            // Create Key = 1
            tps_test::create_counter(&mut state, ts::ctx(&mut scenario));
            // Create Key = 2
            tps_test::create_counter(&mut state, ts::ctx(&mut scenario));

            assert!(tps_test::get_total_created(&state) == 3, 0);

            ts::return_shared(state);
        };

        // =========================================================
        // Step 3: Indexing Phase - Client Queries ID
        // =========================================================
        ts::next_tx(&mut scenario, USER);
        let target_id = {
            let state = ts::take_shared<GlobalState>(&scenario);
            let id = tps_test::get_counter_id(&state, 1);
            ts::return_shared(state);
            id
        };

        // =========================================================
        // Step 4: Run Phase (TPS Run) - High-Frequency Operations
        // =========================================================
        ts::next_tx(&mut scenario, USER);
        {
            // Directly get specific shared Counter object by ID
            let mut counter = ts::take_shared_by_id<Counter>(&scenario, target_id);

            // Verify initial value
            assert!(tps_test::get_value(&counter) == 0, 1);

            // Simulate a PTB containing 1000 operations
            let mut i = 0;
            while (i < 1000) {
                tps_test::operate(&mut counter, ts::ctx(&mut scenario));
                i = i + 1;
            };

            // Verify final result
            assert!(tps_test::get_value(&counter) == 1000, 2);

            ts::return_shared(counter);
        };

        // =========================================================
        // Step 5: Verify No Interference
        // =========================================================
        ts::next_tx(&mut scenario, USER);
        {
            let state = ts::take_shared<GlobalState>(&scenario);
            let id_0 = tps_test::get_counter_id(&state, 0);
            ts::return_shared(state); 

            let counter_0 = ts::take_shared_by_id<Counter>(&scenario, id_0);
            assert!(tps_test::get_value(&counter_0) == 0, 3);
            
            ts::return_shared(counter_0);
        };

        ts::end(scenario);
    }
}