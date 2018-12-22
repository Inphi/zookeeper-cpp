#set(srcs simple.cc)

add_executable(simple ${CMAKE_CURRENT_LIST_DIR}/simple.cc)

target_include_directories(simple PRIVATE ${CMAKE_CURRENT_LIST_DIR})

target_link_libraries (simple PRIVATE zk-cpp)

target_compile_definitions (simple PUBLIC 
  BOOST_COROUTINES_NO_DEPRECATION_WARNING=1 _SCL_SECURE_NO_WARNINGS=1 ZK_LOGGING=1)