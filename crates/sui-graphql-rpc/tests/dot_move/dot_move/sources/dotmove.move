
module dotmove::dotmove {
    use std::string::String;
    use sui::{
        vec_map::{Self, VecMap},
        table::{Self, Table},
    };
    use dotmove::name::{Self, Name};

    public struct AppInfo has copy, store, drop {
        package_info_id: Option<ID>,
        package_address: Option<address>,
        upgrade_cap_id: Option<ID>
    }

    public struct AppRecord has store {
        // The Capability object used for managing the `AppRecord`.
        app_cap_id: ID,
        // The mainnet `AppInfo` object.
        // This is optional until a `mainnet` package is mapped to a record, making 
        // the record immutable.
        app_info: Option<AppInfo>,
        // This is what being resolved across networks.
        networks: VecMap<String, AppInfo>,
        // Any read-only metadata for the record.
        metadata: VecMap<String, String>,
        // Any extra data that needs to be stored.
        // Unblocks TTO, and DFs extendability.
        storage: UID,
    }

    /// The shared object holding the registry of packages.
    /// There are no "admin" actions for this registry.
    public struct AppRegistry has key {
        id: UID,
        registry: Table<Name, AppRecord>
    }

    fun init(ctx: &mut TxContext) {
        transfer::share_object(AppRegistry {
            id: object::new(ctx),
            registry: table::new(ctx),
        });
    }

    public fun add_record(
        registry: &mut AppRegistry,
        name: String,
        package_info_id: ID,
        package_address: address,
        ctx: &mut TxContext
    ) {
        registry.registry.add(name::new(name), AppRecord {
            app_cap_id: @0x0.to_id(),
            app_info: option::some(AppInfo {
                package_info_id: option::some(package_info_id),
                package_address: option::some(package_address),
                upgrade_cap_id: option::none()
            }),
            networks: vec_map::empty(),
            metadata: vec_map::empty(),
            storage: object::new(ctx),
        });
    }

   /// Sets a network's value for a given app name.
    public fun set_network(
        registry: &mut AppRegistry,
        name: String,
        chain_id: String,
        package_info_id: ID,
        package_address: address,
    ) {
        let on_chain_name = name::new(name);
        let record = registry.registry.borrow_mut(on_chain_name);
        if (record.networks.contains(&chain_id)) {
            record.networks.remove(&chain_id);
        };
        record.networks.insert(chain_id, AppInfo {
            package_info_id: option::some(package_info_id),
            package_address: option::some(package_address),
            upgrade_cap_id: option::none()
        });
    }
}
