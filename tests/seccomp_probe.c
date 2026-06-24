#include <errno.h>
#include <linux/bpf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/syscall.h>
#include <unistd.h>
int main(int argc, char **argv) {
  int deny_mount = argc > 1 && strcmp(argv[1], "deny") == 0;
  int descriptors[2];
  if (pipe(descriptors))
    return 1;
  if (write(descriptors[1], "x", 1) != 1)
    return 2;
  int available = 0;
  if (ioctl(descriptors[0], FIONREAD, &available) || available != 1)
    return 3;
  close(descriptors[0]);
  close(descriptors[1]);
  union bpf_attr attributes = {};
  attributes.map_type = BPF_MAP_TYPE_ARRAY;
  attributes.key_size = 4;
  attributes.value_size = 4;
  attributes.max_entries = 1;
  int map = syscall(SYS_bpf, BPF_MAP_CREATE, &attributes, sizeof(attributes));
  if (map < 0) {
    perror("BPF_MAP_CREATE");
    return 4;
  }
  close(map);
  char directory[] = "/tmp/reclaimnet-seccomp-XXXXXX";
  if (!mkdtemp(directory))
    return 5;
  int result = mount("none", directory, "tmpfs", 0, "size=4096");
  int error = errno;
  if (!result)
    umount(directory);
  rmdir(directory);
  if ((deny_mount && (result != -1 || error != EPERM)) ||
      (!deny_mount && result != 0))
    return 6;
  printf("ioctl=allowed bpf_map_create=allowed mount=%s\n",
         deny_mount ? "denied" : "allowed");
  return 0;
}
