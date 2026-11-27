/* Keep the pinned HEART executable usable in a network namespace that omits
 * busy_read. DAO passes the failed lookup as a negative SO_BUSY_POLL budget.
 * Only this invalid budget is replaced. The real syscall decides success. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <sys/socket.h>
#include <string.h>
#include <unistd.h>

int setsockopt(int fd, int level, int option, const void *value, socklen_t size)
{
	int (*set_option)(int, int, int, const void *, socklen_t) =
		dlsym(RTLD_NEXT, "setsockopt");
	if (set_option == NULL) {
		errno = ENOSYS;
		return -1;
	}
	if (level == SOL_SOCKET && option == SO_BUSY_POLL &&
			size == sizeof(int) && value != NULL) {
		int microseconds;
		memcpy(&microseconds, value, size);
		if (microseconds < 0 && access("/proc/sys/net/core/busy_read", F_OK) == -1) {
			const int disabled = 0;
			fputs("HEART test: negative SO_BUSY_POLL budget; disabling busy polling\n", stderr);
			return set_option(fd, level, option, &disabled, sizeof(disabled));
		}
	}
	return set_option(fd, level, option, value, size);
}
