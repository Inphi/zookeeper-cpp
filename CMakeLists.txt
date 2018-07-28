cmake_minimum_required (VERSION 3.5.2)

project (zk-cpp VERSION 1)

set_property (GLOBAL PROPERTY USE_FOLDERS ON)
option (ZK_CPP_BUILD_SAMPLES "Build samples" ON)

set (THREADS_PREFER_PTHREAD_FLAG ON)
find_package (Threads)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (UNIX)
  set( CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -Wno-unused-parameter")
endif()

option (Boost_USE_STATIC_LIBS "Use static libraries for boost" ON)

set(BOOST_COMPONENTS system)
find_package (Boost 1.58.0 REQUIRED COMPONENTS ${BOOST_COMPONENTS})

add_library (${PROJECT_NAME} INTERFACE)
add_library (${PROJECT_NAME}::${PROJECT_NAME} ALIAS ${PROJECT_NAME})
target_link_libraries (${PROJECT_NAME} INTERFACE ${Boost_LIBRARIES})
target_link_libraries (${PROJECT_NAME} INTERFACE Threads::Threads)
target_include_directories(${PROJECT_NAME} INTERFACE ${PROJECT_SOURCE_DIR}/include)

install(DIRECTORY "${PROJECT_SOURCE_DIR}/include/" DESTINATION "include")

if (ZK_CPP_BUILD_SAMPLES)
  add_subdirectory(examples)
endif()
