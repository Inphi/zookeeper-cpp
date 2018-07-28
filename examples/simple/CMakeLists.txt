#set(srcs simple.cc)

add_executable(simple simple.cc)

list(APPEND BOOST_COMPONENTS coroutine context thread)
find_package (Boost 1.58.0 REQUIRED COMPONENTS ${BOOST_COMPONENTS})
link_directories(${Boost_LIBRARY_DIRS})

target_link_libraries (${PROJECT_NAME} INTERFACE ${Boost_LIBRARIES})

target_compile_definitions (${PROJECT_NAME} INTERFACE
  BOOST_COROUTINES_NO_DEPRECATION_WARNING=1)
target_link_libraries(simple zk-cpp)
