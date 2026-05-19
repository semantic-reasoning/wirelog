import wirelog

private let smokeReadCallback: @convention(c) (
    OpaquePointer?,
    UnsafeMutablePointer<UnsafeMutablePointer<Int64>?>?,
    UnsafeMutablePointer<UInt32>?,
    UnsafeMutableRawPointer?
) -> Int32 = { ctx, outData, outRows, userData in
    _ = ctx
    _ = outData
    _ = userData
    outRows?.pointee = 0
    return 0
}

private let smokeScheme = "swift-smoke"
private let smokeDescription = "Swift modulemap smoke adapter"

private func exerciseImportedAPI() {
    smokeScheme.withCString { scheme in
        smokeDescription.withCString { description in
            var adapter = wirelog_io_adapter_t(
                abi_version: WIRELOG_IO_ABI_VERSION,
                scheme: scheme,
                description: description,
                read: smokeReadCallback,
                validate: nil,
                user_data: nil
            )

            _ = wirelog_io_register_adapter(&adapter)
            _ = wirelog_io_find_adapter(scheme)
            _ = wirelog_io_unregister_adapter(scheme)
        }
    }
}
