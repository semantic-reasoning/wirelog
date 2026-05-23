cmake_minimum_required(VERSION 3.22)
project(asset_adapter LANGUAGES C)

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
