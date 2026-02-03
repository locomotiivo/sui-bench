module bloat_storage::bloat {
    use sui::object;
    use sui::tx_context;
    use sui::transfer;
    use std::vector;
    use std::hash;

    /// Large storage blob - configurable size
    public struct Blob has key, store {
        id: object::UID,
        data: vector<u8>,        // Main payload
        metadata: vector<u8>,    // Extra metadata
        counter: u64,
        timestamp: u64,
        owner: address,
    }

    /// Simple PRNG using xorshift - overflow-safe for Move
    /// Generates pseudo-random bytes that are incompressible
    fun next_random(seed: &mut u64): u8 {
        // xorshift64 - simple and overflow-safe
        let mut x = *seed;
        x = x ^ (x << 13);
        x = x ^ (x >> 7);
        x = x ^ (x << 17);
        *seed = x;
        // Take low byte
        (x & 0xFF) as u8
    }

    /// Generate incompressible random data from a seed
    fun generate_random_data(bytes: u64, seed: u64): vector<u8> {
        let mut data = vector::empty<u8>();
        // Ensure seed is never 0 (xorshift requires non-zero)
        let mut current_seed = if (seed == 0) { 0xDEADBEEF } else { seed };
        let mut i = 0;
        
        while (i < bytes) {
            vector::push_back(&mut data, next_random(&mut current_seed));
            i = i + 1;
        };
        
        data
    }

    /// Create blob with INCOMPRESSIBLE random data (for storage bloat testing)
    /// Uses tx digest hash + epoch + counter + UID as seed for truly unique random generation
    public entry fun create_blob(size_kb: u64, ctx: &mut tx_context::TxContext) {
        let bytes = size_kb * 1024;
        
        // Create UID first - this is unique per object
        let uid = object::new(ctx);
        
        // Create unique seed from transaction context + UID bytes
        let epoch = tx_context::epoch(ctx);
        let sender = tx_context::sender(ctx);
        let sender_bytes = std::bcs::to_bytes(&sender);
        
        // Get UID bytes for additional entropy
        let uid_bytes = object::uid_to_bytes(&uid);
        
        // Hash sender + epoch + timestamp + UID for truly unique seed
        let mut seed_data = sender_bytes;
        vector::append(&mut seed_data, std::bcs::to_bytes(&epoch));
        vector::append(&mut seed_data, std::bcs::to_bytes(&tx_context::epoch_timestamp_ms(ctx)));
        vector::append(&mut seed_data, uid_bytes);  // UID makes each blob unique
        let hash_bytes = hash::sha2_256(seed_data);
        
        // Convert first 8 bytes of hash to u64 seed
        let mut seed: u64 = 0;
        let mut j = 0;
        while (j < 8) {
            seed = (seed << 8) | (*vector::borrow(&hash_bytes, j) as u64);
            j = j + 1;
        };
        
        // Generate incompressible random data
        let data = generate_random_data(bytes, seed);

        let blob = Blob {
            id: uid,
            data,
            metadata: vector::empty(),
            counter: 1,
            timestamp: epoch,
            owner: sender,
        };

        transfer::transfer(blob, tx_context::sender(ctx));
    }
    
    /// Legacy: Create blob with compressible pattern (for comparison)
    public entry fun create_blob_compressible(size_kb: u64, ctx: &mut tx_context::TxContext) {
        let bytes = size_kb * 1024;
        let mut data = vector::empty<u8>();
        let mut i = 0;
        
        while (i < bytes) {
            vector::push_back(&mut data, ((i % 256) as u8));
            i = i + 1;
        };

        let blob = Blob {
            id: object::new(ctx),
            data,
            metadata: vector::empty(),
            counter: 1,
            timestamp: tx_context::epoch(ctx),
            owner: tx_context::sender(ctx),
        };

        transfer::transfer(blob, tx_context::sender(ctx));
    }

    /// Create multiple blobs in one PTB (uses incompressible random data)
    /// Each blob gets a unique seed based on its index in the batch
    public entry fun create_blobs_batch(
        size_kb: u64, 
        count: u64, 
        ctx: &mut tx_context::TxContext
    ) {
        let mut i = 0;
        while (i < count) {
            create_blob_with_index(size_kb, i, ctx);
            i = i + 1;
        };
    }
    
    /// Create blob with unique index to ensure different random data per blob
    fun create_blob_with_index(size_kb: u64, index: u64, ctx: &mut tx_context::TxContext) {
        let bytes = size_kb * 1024;
        
        // Create UID first - this is unique per object
        let uid = object::new(ctx);
        
        // Create unique seed from transaction context + index + UID
        let epoch = tx_context::epoch(ctx);
        let sender = tx_context::sender(ctx);
        let sender_bytes = std::bcs::to_bytes(&sender);
        
        // Get UID bytes for additional entropy
        let uid_bytes = object::uid_to_bytes(&uid);
        
        // Hash sender + epoch + timestamp + index + UID for truly unique seed
        let mut seed_data = sender_bytes;
        vector::append(&mut seed_data, std::bcs::to_bytes(&epoch));
        vector::append(&mut seed_data, std::bcs::to_bytes(&tx_context::epoch_timestamp_ms(ctx)));
        vector::append(&mut seed_data, std::bcs::to_bytes(&index));
        vector::append(&mut seed_data, uid_bytes);  // UID makes each blob truly unique
        let hash_bytes = hash::sha2_256(seed_data);
        
        // Convert first 8 bytes of hash to u64 seed
        let mut seed: u64 = 0;
        let mut j = 0;
        while (j < 8) {
            seed = (seed << 8) | (*vector::borrow(&hash_bytes, j) as u64);
            j = j + 1;
        };
        
        // Generate incompressible random data
        let data = generate_random_data(bytes, seed);

        let blob = Blob {
            id: uid,
            data,
            metadata: vector::empty(),
            counter: 1,
            timestamp: epoch,
            owner: sender,
        };

        transfer::transfer(blob, tx_context::sender(ctx));
    }

    /// Update blob with INCOMPRESSIBLE random data (creates new version = storage churn)
    public entry fun update_blob(blob: &mut Blob, new_size_kb: u64, ctx: &mut tx_context::TxContext) {
        let bytes = new_size_kb * 1024;
        
        // Generate new random seed from context
        let sender_bytes = std::bcs::to_bytes(&tx_context::sender(ctx));
        let mut seed_data = sender_bytes;
        vector::append(&mut seed_data, std::bcs::to_bytes(&tx_context::epoch(ctx)));
        vector::append(&mut seed_data, std::bcs::to_bytes(&blob.counter));
        let hash_bytes = hash::sha2_256(seed_data);
        
        let mut seed: u64 = 0;
        let mut j = 0;
        while (j < 8) {
            seed = (seed << 8) | (*vector::borrow(&hash_bytes, j) as u64);
            j = j + 1;
        };
        
        blob.data = generate_random_data(bytes, seed);
        blob.counter = blob.counter + 1;
    }

    /// Delete blob (for churn testing)
    public entry fun delete_blob(blob: Blob) {
        let Blob { id, data: _, metadata: _, counter: _, timestamp: _, owner: _ } = blob;
        object::delete(id);
    }

    /// Mass create with different sizes for fragmentation
    public entry fun create_varied_blobs(
        base_size_kb: u64,
        count: u64,
        ctx: &mut tx_context::TxContext
    ) {
        let mut i = 0;
        while (i < count) {
            // Vary size: 50%, 100%, 150%, 200% of base
            let multiplier = (i % 4) + 1;
            let size = base_size_kb * multiplier / 2;
            create_blob(size, ctx);
            i = i + 1;
        };
    }
}
