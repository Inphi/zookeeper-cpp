cmake_minimum_required (VERSION 3.5)

project (zk-cpp VERSION 0.0.1)

set_property (GLOBAL PROPERTY USE_FOLDERS ON)
option (ZK_CPP_BUILD_SAMPLES "Build samples" ON)

set (THREADS_PREFER_PTHREAD_FLAG ON)
find_package (Threads)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (UNIX)
  set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -Wno-unused-parameter")
endif()

set(BOOST_COMPONENTS system)
if (ZK_CPP_BUILD_SAMPLES)
  list(APPEND BOOST_COMPONENTS coroutine)
endif()
find_package (Boost 1.66.0 REQUIRED COMPONENTS ${BOOST_COMPONENTS})

add_library (${PROJECT_NAME} INTERFACE)
target_link_libraries (${PROJECT_NAME} INTERFACE ${Boost_LIBRARIES})
target_link_libraries (${PROJECT_NAME} INTERFACE Threads::Threads)
target_include_directories(${PROJECT_NAME} INTERFACE ${PROJECT_SOURCE_DIR}/include)
target_include_directories(${PROJECT_NAME} SYSTEM INTERFACE ${Boost_INCLUDE_DIRS})

install(DIRECTORY "${PROJECT_SOURCE_DIR}/include/" DESTINATION "include")

if (ZK_CPP_BUILD_SAMPLES)
  add_subdirectory(examples)
endif()
