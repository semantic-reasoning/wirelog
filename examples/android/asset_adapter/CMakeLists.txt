find_package(wirelog REQUIRED CONFIG)

add_library(asset_adapter SHARED
    asset_adapter.c
)

target_link_libraries(asset_adapter
    PRIVATE
        wirelog::wirelog
        android
        log
)
