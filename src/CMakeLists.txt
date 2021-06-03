add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/API)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/BufferManager)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/CatalogManager)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/IndexManager)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/Interpreter)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/RecordManager)

set(SOURCE_FILES ${SOURCE_FILES} ${CMAKE_CURRENT_SOURCE_DIR}/main.cc PARENT_SCOPE)
