find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/reclaimnet.proto")

get_filename_component(PROTO_PATH "${PROTO_FILE}" PATH)
get_filename_component(PROTO_BASENAME "${PROTO_FILE}" NAME_WE)

set(PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_BASENAME}.pb.cc")
set(PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_BASENAME}.pb.h")
set(GRPC_SRCS  "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_BASENAME}.grpc.pb.cc")
set(GRPC_HDRS  "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_BASENAME}.grpc.pb.h")

get_target_property(GRPC_CPP_PLUGIN gRPC::grpc_cpp_plugin LOCATION)

add_custom_command(
  OUTPUT "${PROTO_SRCS}" "${PROTO_HDRS}" "${GRPC_SRCS}" "${GRPC_HDRS}"
  COMMAND protobuf::protoc
  ARGS
    --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
    --cpp_out  "${CMAKE_CURRENT_BINARY_DIR}"
    -I "${PROTO_PATH}"
    --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN}"
    "${PROTO_FILE}"
  DEPENDS "${PROTO_FILE}"
  COMMENT "Generating protobuf/gRPC sources from ${PROTO_FILE}"
)

add_library(reclaimnet_proto STATIC
  "${PROTO_SRCS}"
  "${GRPC_SRCS}"
)

target_include_directories(reclaimnet_proto PUBLIC
  "${CMAKE_CURRENT_BINARY_DIR}"
)

target_link_libraries(reclaimnet_proto PUBLIC
  protobuf::libprotobuf
  gRPC::grpc++
)
