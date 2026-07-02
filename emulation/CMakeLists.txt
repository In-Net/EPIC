cmake_minimum_required(VERSION 3.16)
project(epic_clean_vm C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

add_compile_options(-O3 -Wall -Wextra -Wno-unused-parameter)

find_package(PkgConfig REQUIRED)
pkg_check_modules(PCAP REQUIRED libpcap)
pkg_check_modules(IBVERBS REQUIRED libibverbs)

include_directories(common/include ${PCAP_INCLUDE_DIRS} ${IBVERBS_INCLUDE_DIRS})

add_library(epic_roce common/src/roce_packet.c)

add_executable(star4_switch common/src/star4_switch.c)
target_link_libraries(star4_switch epic_roce ${PCAP_LIBRARIES})

add_executable(primitive_host common/src/primitive_host.c)
target_link_libraries(primitive_host epic_roce ${IBVERBS_LIBRARIES})

add_executable(mode1_node common/src/mode1_node.c)
target_link_libraries(mode1_node epic_roce ${IBVERBS_LIBRARIES})

add_executable(pcap_l2_switch benchmarks/src/pcap_l2_switch.c)
target_link_libraries(pcap_l2_switch epic_roce ${PCAP_LIBRARIES})

add_executable(pure_pingpong benchmarks/src/pure_pingpong.c)
target_link_libraries(pure_pingpong ${IBVERBS_LIBRARIES})
