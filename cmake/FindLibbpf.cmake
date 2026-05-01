


find_path(LIBBPF_INCLUDE_DIR bpf/libbpf.h
  HINTS /usr/include /usr/local/include /opt/homebrew/include)

find_library(LIBBPF_LIBRARY NAMES bpf
  HINTS /usr/lib /usr/lib/x86_64-linux-gnu /usr/local/lib /opt/homebrew/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Libbpf DEFAULT_MSG LIBBPF_LIBRARY LIBBPF_INCLUDE_DIR)

if(Libbpf_FOUND AND NOT TARGET libbpf::libbpf)
  add_library(libbpf::libbpf UNKNOWN IMPORTED)
  set_target_properties(libbpf::libbpf PROPERTIES
    IMPORTED_LOCATION "${LIBBPF_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${LIBBPF_INCLUDE_DIR}")
endif()

mark_as_advanced(LIBBPF_INCLUDE_DIR LIBBPF_LIBRARY)
