/*-
 * Copyright (c) 2013  Peter Grehan <grehan@freebsd.org>
 * Copyright (c) 2015 xhyve developers
 * Copyright (c) 2016 Johannes Schriewer
 * Copyright (c) 2016 Daniel Borca
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * $FreeBSD$
 */

#include <sys/param.h>
#include <sys/queue.h>
#include <sys/errno.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/disk.h>

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>

#include <xhyve/support/atomic.h>
#include <xhyve/xhyve.h>
#include <xhyve/block_if.h>
#include <xhyve/vdsk/vdsk.h>
#include <xhyve/vdsk/vdsk-int.h>
#include <xhyve/vdsk/vdsk-split.h>

struct vdsk_split_ctx {
	struct vdsk super;
	// backing file fds
	int *bc_fd;
	int bc_num_fd;
	int bc_ischr;
	// sparse lookup table
	int bc_sparse;
	int bc_sparse_fd;
	uint32_t *bc_sparse_lut;
	size_t bc_split;
};

/* xhyve: FIXME
 *
 * As split disk images probably need multiple reads and writes we can not
 * use preadv/pwritev, we need to serialize reads and writes
 * for the time being until we find a better solution.
 */

static int
blockif_get_fd(const struct vdsk_split_ctx *vp, size_t offset)
{
	if (vp->bc_split) {
		int i = (int)(offset / vp->bc_split);
		return vp->bc_fd[i];
	} else {
		return vp->bc_fd[0];
	}
}

static ssize_t
blockif_sparse_read(const struct vdsk_split_ctx *vp, size_t disk_offset, size_t segment_offset, uint8_t *buf, size_t len)
{
	const struct vdsk *vdsk = (const struct vdsk *)vp;

	int fd = blockif_get_fd(vp, disk_offset);
	size_t sector_size = (size_t)vdsk->bc_sectsz;
	size_t block = 0;

	if (vp->bc_split) {
		// split file image, add file segment offset
		size_t i = (disk_offset / (size_t)vp->bc_split);
		block += i * (vp->bc_split / sector_size);
	}

	// read
	size_t remaining = len;
	while (remaining > 0) {
		size_t current_block = block + (segment_offset / sector_size);
		if (current_block > (size_t)vdsk->bc_size / sector_size) {
			fprintf(stderr, "reading past end of disk, requested block %zu of %zu (offset: %zu)\n", current_block, (size_t)vdsk->bc_size / sector_size, disk_offset);
			errno = EFAULT;
			return -1;
		}
		// fprintf(stderr, "reading block %zu/%zu -> offset %d\n", current_block, (size_t)vdsk->bc_size / sector_size, vp->bc_sparse_lut[current_block]);

		size_t shift_offset = segment_offset % sector_size; // offset _in_ a sector
		size_t read_len = (remaining > sector_size) ? sector_size : remaining;

		if (vp->bc_sparse_lut[current_block] < 0xffffffff) {
			// allocated block, get offset and read from file
			size_t seek_offset = vp->bc_sparse_lut[current_block] * sector_size + shift_offset;
			lseek(fd, (off_t)seek_offset, SEEK_SET);

			ssize_t result = read(fd, buf, read_len - shift_offset);
			// fprintf(stderr, "sparse read disk %zu, segment %zu, len %zu -> read %zu bytes\n", disk_offset, segment_offset, read_len, result);
			if (result < 0) {
				return result;
			}
		} else {
			// sparse block, fill buffer with zeroes
			// fprintf(stderr, "sparse read disk %zu, segment %zu, len %zu -> memset\n", disk_offset, segment_offset, read_len);
			memset(buf, 0, read_len - shift_offset);
		}

		// advance buffer
		if (remaining > sector_size - shift_offset) {
			remaining -= sector_size - shift_offset;
			buf += sector_size - shift_offset;
			segment_offset += sector_size - shift_offset;
		} else {
			remaining = 0;
		}
	}

	return (ssize_t)len;
}

static ssize_t
blockif_sparse_write(const struct vdsk_split_ctx *vp, size_t disk_offset, size_t segment_offset, uint8_t *buf, size_t len)
{
	const struct vdsk *vdsk = (const struct vdsk *)vp;

	int fd = blockif_get_fd(vp, disk_offset);
	size_t sector_size = (size_t)vdsk->bc_sectsz;
	size_t block = 0;

	if (vp->bc_split) {
		// split file image, add file segment offset
		size_t i = (disk_offset / (size_t)vp->bc_split);
		block += i * (vp->bc_split / sector_size);
	}

	// read
	size_t remaining = len;
	while (remaining > 0) {
		size_t current_block = block + (segment_offset / sector_size);
		if (current_block > (size_t)vdsk->bc_size / sector_size) {
			fprintf(stderr, "writing past end of disk, requested block %zu of %zu (offset: %zu)\n", current_block, (size_t)vdsk->bc_size / sector_size, disk_offset);
			errno = EFAULT;
			return -1;
		}
		// fprintf(stderr, "writing block %zu\n", current_block);

		size_t shift_offset = segment_offset % sector_size; // offset _in_ a sector
		size_t write_len = (remaining > sector_size) ? sector_size : remaining;

		if (vp->bc_sparse_lut[current_block] < 0xffffffff) {
			// allocated block, get offset and read from file
			size_t seek_offset = vp->bc_sparse_lut[current_block] * sector_size + shift_offset;
			lseek(fd, (off_t)seek_offset, SEEK_SET);

			ssize_t result = write(fd, buf, write_len - shift_offset);
			// fprintf(stderr, "sparse write disk %zu, segment %zu, len %zu -> existing block %zu bytes\n", disk_offset, segment_offset, write_len, result);
			if (result < 0) {
				return result;
			}
		} else {
			// sparse block, append to file
			struct stat sbuf;
			ssize_t result;

			// check if the buffer is zeroes only
			int zeroes_only = 1;
			for (uint8_t *ptr = buf; ptr < buf + write_len - shift_offset; ptr++) {
				if (*ptr != 0) {
					zeroes_only = 0;
					break;
				}
			}
			if (!zeroes_only) {
				// save sector offset into lut
				fstat(fd, &sbuf);
				vp->bc_sparse_lut[current_block] = (uint32_t)((size_t)sbuf.st_size / (size_t)sector_size);
				lseek(vp->bc_sparse_fd, (off_t)(current_block * 4), SEEK_SET);
				result = write(vp->bc_sparse_fd, vp->bc_sparse_lut + current_block, 4);
				if (result < 0) {
					return result;
				}
				fsync(vp->bc_sparse_fd);

				// create sector
				lseek(fd, 0, SEEK_END);
				char zeroBuffer[sector_size];
				memset(zeroBuffer, 0, sector_size);
				write(fd, zeroBuffer, sector_size);

				// overwrite with data
				size_t seek_offset = vp->bc_sparse_lut[current_block] * sector_size + shift_offset;
				lseek(fd, (off_t)seek_offset, SEEK_SET);
				result = write(fd, buf, write_len - shift_offset);
				// fprintf(stderr, "sparse write disk %zu, segment %zu, len %zu -> new block %zu bytes\n", disk_offset, segment_offset, write_len, result);
				if (result < 0) {
					return result;
				}
				fsync(fd);
			}
		}

		// advance buffer
		if (remaining > sector_size - shift_offset) {
			remaining -= sector_size - shift_offset;
			buf += sector_size - shift_offset;
			segment_offset += sector_size - shift_offset;
		} else {
			remaining = 0;
		}
	}

	return (ssize_t)len;
}

static ssize_t
blockif_read_data(const struct vdsk_split_ctx *vp, uint8_t *buf, size_t len, size_t offset)
{
	// find correct fd
	int fd = blockif_get_fd(vp, offset);
	ssize_t bytes = 0;

	// fprintf(stderr, "read %zu bytes at %zu\n", len, offset);

	if (!vp->bc_sparse) {
		if (vp->bc_split) {
			lseek(fd, (off_t)(offset % vp->bc_split), SEEK_SET);
		} else {
			lseek(fd, (off_t)offset, SEEK_SET);
		}
	}

	// is this a multi part read
	if ((vp->bc_split) && (offset % vp->bc_split + len > vp->bc_split)) {
		// read is longer than current segment

		// read until end of segment
		size_t len1 = vp->bc_split - (offset % vp->bc_split);
		if (vp->bc_sparse) {
			bytes = blockif_sparse_read(vp, offset, (offset % vp->bc_split), buf, len1);
		} else {
			bytes = read(fd, buf, len1);
		}
		if (bytes < 0) {
			return bytes;
		}

		// get next fd and read the rest
		size_t len2 = len - len1;
		fd = blockif_get_fd(vp, offset + len1);

		ssize_t result;
		if (vp->bc_sparse) {
			result = blockif_sparse_read(vp, offset + len1, 0, buf + len1, len2);
		} else {
			lseek(fd, 0, SEEK_SET);
			result = read(fd, buf + len1, len2);
		}
		if (result < 0) {
			return result;
		}
		bytes += result;
	} else {
		// read does not cross segment border
		if (vp->bc_sparse) {
			bytes = blockif_sparse_read(vp, offset, (offset % vp->bc_split), buf, len);
		} else {
			bytes = read(fd, buf, len);
		}
	}

	// return read bytes
	return bytes;
}

static ssize_t
blockif_write_data(const struct vdsk_split_ctx *vp, uint8_t *buf, size_t len, size_t offset)
{
	// find correct fd
	int fd = blockif_get_fd(vp, offset);
	ssize_t bytes = 0;

	// fprintf(stderr, "write %zu bytes at %zu\n", len, offset);

	if (!vp->bc_sparse) {
		if (vp->bc_split) {
			lseek(fd, (off_t)(offset % vp->bc_split), SEEK_SET);
		} else {
			lseek(fd, (off_t)offset, SEEK_SET);
		}
	}

	// is this a multi part write
	if ((vp->bc_split) && (offset % vp->bc_split + len > vp->bc_split)) {
		// write is longer than current segment

		// write until end of segment
		size_t len1 = vp->bc_split - (offset % vp->bc_split);
		if (vp->bc_sparse) {
			bytes = blockif_sparse_write(vp, offset, (offset % vp->bc_split), buf, len1);
		} else {
			bytes = write(fd, buf, len1);
		}
		if (bytes < 0) {
			return bytes;
		}

		// get next fd and write the rest
		size_t len2 = len - len1;
		fd = blockif_get_fd(vp, offset + len1);

		ssize_t result;
		if (vp->bc_sparse) {
			result = blockif_sparse_write(vp, offset + len1, 0, buf + len1, len2);
		} else {
			lseek(fd, 0, SEEK_SET);
			result = write(fd, buf + len1, len2);
		}

		if (result < 0) {
			return result;
		}
		bytes += result;
	} else {
		// write does not cross segment border
		if (vp->bc_sparse) {
			bytes = blockif_sparse_write(vp, offset, (offset % vp->bc_split), buf, len);
		} else {
			bytes = write(fd, buf, len);
		}
	}

	// return written bytes
	return bytes;
}

static int
disk_close(struct vdsk *vdsk)
{
	struct vdsk_split_ctx *vp = (struct vdsk_split_ctx *)vdsk;

	int i;

	for (i = 0; i < vp->bc_num_fd; i++) {
		close(vp->bc_fd[i]);
	}
	free(vp->bc_fd);

	if (vp->bc_sparse) {
		close(vp->bc_sparse_fd);
		free(vp->bc_sparse_lut);
	}

	free(vp);

	return (0);
}

static int
disk_read(const struct vdsk *vdsk, struct blockif_req *br, uint8_t *buf)
{
	const struct vdsk_split_ctx *vp = (const struct vdsk_split_ctx *)vdsk;

	ssize_t clen, len, off, boff, voff;
	int i, err;

	err = 0;

	if (buf == NULL) {
		// as we have to account for split disk images we disassemble
		// the iovec buffers and call read for each of them
		size_t offset = (size_t)br->br_offset;
		for (i = 0; i < br->br_iovcnt; i++) {
			len = blockif_read_data(vp, br->br_iov[i].iov_base, br->br_iov[i].iov_len, offset);
			if (len < 0) {
				err = errno;
			} else {
				br->br_resid -= len;
			}
			offset += br->br_iov[i].iov_len;
		}
		return err;
	}
	i = 0;
	off = voff = 0;
	while (br->br_resid > 0) {
		len = MIN(br->br_resid, MAXPHYS);
		if (blockif_read_data(vp, buf, ((size_t) len), (size_t)(br->br_offset + off)) < 0)
		{
			err = errno;
			break;
		}
		boff = 0;
		do {
			clen = MIN((len - boff),
				(((ssize_t) br->br_iov[i].iov_len) - voff));
			memcpy(((void *) (((uintptr_t) br->br_iov[i].iov_base) +
				((size_t) voff))), buf + boff, clen);
			if (clen < (((ssize_t) br->br_iov[i].iov_len) - voff))
				voff += clen;
			else {
				i++;
				voff = 0;
			}
			boff += clen;
		} while (boff < len);
		off += len;
		br->br_resid -= len;
	}

	return err;
}

static int
disk_write(const struct vdsk *vdsk, struct blockif_req *br, uint8_t *buf)
{
	const struct vdsk_split_ctx *vp = (const struct vdsk_split_ctx *)vdsk;

	ssize_t clen, len, off, boff, voff;
	int i, err;

	err = 0;

	if (vdsk->bc_rdonly) {
		err = EROFS;
		return err;
	}
	if (buf == NULL) {
		// as we have to account for split disk images we disassemble
		// the iovec buffers and call write for each of them
		size_t offset = (size_t)br->br_offset;
		for (i = 0; i < br->br_iovcnt; i++) {
			len = blockif_write_data(vp, br->br_iov[i].iov_base, br->br_iov[i].iov_len, offset);
			if (len < 0) {
				err = errno;
			} else {
				br->br_resid -= len;
			}
			offset += br->br_iov[i].iov_len;
		}
		return err;
	}
	i = 0;
	off = voff = 0;
	while (br->br_resid > 0) {
		len = MIN(br->br_resid, MAXPHYS);
		boff = 0;
		do {
			clen = MIN((len - boff),
				(((ssize_t) br->br_iov[i].iov_len) - voff));
			memcpy((buf + boff),
				((void *) (((uintptr_t) br->br_iov[i].iov_base) +
					((size_t) voff))), clen);
			if (clen < (((ssize_t) br->br_iov[i].iov_len) - voff))
				voff += clen;
			else {
				i++;
				voff = 0;
			}
			boff += clen;
		} while (boff < len);
		if (blockif_write_data(vp, buf, ((size_t) len), (size_t)(br->br_offset +
		    off)) < 0) {
			err = errno;
			break;
		}
		off += len;
		br->br_resid -= len;
	}

	return err;
}

static int
disk_flush(const struct vdsk *vdsk)
{
	const struct vdsk_split_ctx *vp = (const struct vdsk_split_ctx *)vdsk;

	int i;
	int err = 0;

	for (i = 0; i < vp->bc_num_fd; i++) {
		if (vp->bc_ischr) {
			if (ioctl(vp->bc_fd[i], DKIOCSYNCHRONIZECACHE))
				err = errno;
		} else if (fsync(vp->bc_fd[i]))
			err = errno;
	}

	return err;
}

static int
disk_delete(const struct vdsk *vdsk, UNUSED struct blockif_req *br)
{
	// const struct vdsk_split_ctx *vp = (const struct vdsk_split_ctx *)vdsk;

	// off_t arg[2];
	int err = 0;

	if (!vdsk->bc_candelete) {
		err = EOPNOTSUPP;
	// } else if (vdsk->bc_rdonly) {
	// 	err = EROFS;
	// } else if (vp->bc_ischr) {
	// 	arg[0] = br->br_offset;
	// 	arg[1] = br->br_resid;
	// 	if (ioctl(vp->bc_fd, DIOCGDELETE, arg)) {
	// 		err = errno;
	// 	} else {
	// 		br->br_resid = 0;
	// 	}
	} else {
		err = EOPNOTSUPP;
	}

	return err;
}

struct vdsk *
vdsk_split_open(const char *optstr, int numthr)
{
	// char name[MAXPATHLEN];
	char *nopt, *xopts, *cp, tmp[255];
	struct vdsk_split_ctx *bc;
	struct stat sbuf;
	// struct diocgattr_arg arg;
	size_t size, psectsz, psectoff, split;
	int extra, fd, sectsz;
	int nocache, sync, ro, candelete, geom, ssopt, pssopt, sparse;
	int *fds, sparse_fd;
	uint32_t *sparse_lut;

	assert(numthr == 1);

	fd = -1;
	sparse_fd = -1;
	fds = NULL;
	sparse_lut = NULL;
	ssopt = 0;
	nocache = 0;
	sync = 0;
	ro = 0;
	size = 0;
	split = 0;
	sparse = 0;

	pssopt = 0;
	/*
	 * The first element in the optstring is always a pathname.
	 * Optional elements follow
	 */
	nopt = xopts = strdup(optstr);
	while (xopts != NULL) {
		cp = strsep(&xopts, ",");
		if (cp == nopt)		/* file or device pathname */
			continue;
		else if (!strcmp(cp, "nocache"))
			nocache = 1;
		else if (!strcmp(cp, "sync") || !strcmp(cp, "direct"))
			sync = 1;
		else if (!strcmp(cp, "ro"))
			ro = 1;
		else if (sscanf(cp, "sectorsize=%d/%d", &ssopt, &pssopt) == 2)
			;
		else if (sscanf(cp, "sectorsize=%d", &ssopt) == 1)
			pssopt = ssopt;
		else if (sscanf(cp, "size=%s", tmp) == 1) {
			uint64_t num = 0;
			if (expand_number(tmp, &num)) {
				perror("xhyve: could not parse size parameter");
				goto err;
			}
			size = (size_t)num;
		} else if (sscanf(cp, "split=%s", tmp) == 1) { /* split into chunks */
			uint64_t num = 0;
			if (expand_number(tmp, &num)) {
				perror("xhyve: could not parse split parameter");
				goto err;
			}
			split = (size_t)num;
		}
		else if (!strcmp(cp, "sparse"))
			sparse = 1;
		else {
			fprintf(stderr, "Invalid device option \"%s\"\n", cp);
			goto err;
		}
	}
	if (!split && !sparse) {
		printf("Skipping %s\n", nopt);
		goto err;
	}

	extra = 0;
	if (nocache) {
		perror("xhyve: nocache support unimplemented");
		goto err;
		// extra |= O_DIRECT;
	}
	if (sync)
		extra |= O_SYNC;

	if (split != 0) {
		// open multiple files
		if (size == 0) {
			perror("xhyve: when using 'split' a 'size' is required!");
			goto err;
		}

		size_t num_parts = size / split;
		fds = malloc(sizeof(int) * num_parts);
		for (size_t i = 0; i < num_parts; i++) {
			fds[i] = -1;
		}

		printf("Split disk, opening %zu image parts\n", num_parts);

		for (size_t i = 0; i < num_parts; i++) {
			size_t len = strlen(nopt) + 6;
			char *filename = calloc(len, 1);
			snprintf(filename, len, "%s.%04zu", nopt, i);

			printf(" - %s\n", filename);

			fd = open(filename, (ro ? O_RDONLY : O_RDWR | O_CREAT) | extra);
			if (fd < 0 && !ro) {
				perror("Could not open backing file r/w, reverting to readonly");
				/* Attempt a r/w fail with a r/o open */
				fd = open(filename, O_RDONLY | extra);
				ro = 1;
			}
			free(filename);

			if (fd < 0) {
				perror("Could not open backing file");
				goto err;
			}

			if (fstat(fd, &sbuf) < 0) {
				perror("Could not stat backing file");
				goto err;
			}

			if (sbuf.st_size == 0) {
				// create image file
				printf("   -> file does not exist, creating empty file\n");
				fchmod(fd, 0660);
				if (!sparse) {
					char buffer[1024];
					memset(buffer, 0, 1024);
					for (size_t j = 0; j < split / 1024; j++) {
						write(fd, buffer, 1024);
					}
				}
				lseek(fd, 0, SEEK_SET);
			}

			fds[i] = fd;
		}
	} else {
		// open a single file
		printf("Single image disk\n");

		fd = open(nopt, (ro ? O_RDONLY : O_RDWR | O_CREAT) | extra);
		if (fd < 0 && !ro) {
			perror("Could not open backing file r/w, reverting to readonly");
			/* Attempt a r/w fail with a r/o open */
			fd = open(nopt, O_RDONLY | extra);
			ro = 1;
		}

		if (fd < 0) {
			perror("Could not open backing file");
			goto err;
		}

		if (fstat(fd, &sbuf) < 0) {
			perror("Could not stat backing file");
			goto err;
		}

		if (size == 0) {
			size = (size_t)sbuf.st_size;
		}
		if (sbuf.st_size == 0) {
			// TODO: make growing disks possible
			// create image file
			printf(" -> file does not exist, creating empty file\n");
			fchmod(fd, 0660);
			if (!sparse) {
				char buffer[1024];
				memset(buffer, 0, 1024);
				for (size_t i = 0; i < size / 1024; i++) {
					write(fd, buffer, 1024);
				}
			}
			lseek(fd, 0, SEEK_SET);
		}
    }

    /*
	 * Deal with raw devices
	 */
	sectsz = DEV_BSIZE;
	psectsz = psectoff = 0;
	candelete = geom = 0;
	if (S_ISCHR(sbuf.st_mode)) {
		perror("xhyve: raw device support unimplemented");
		goto err;		
		// if (ioctl(fd, DIOCGMEDIASIZE, &size) < 0 ||
		// 	ioctl(fd, DIOCGSECTORSIZE, &sectsz))
		// {
		// 	perror("Could not fetch dev blk/sector size");
		// 	goto err;
		// }
		// assert(size != 0);
		// assert(sectsz != 0);
		// if (ioctl(fd, DIOCGSTRIPESIZE, &psectsz) == 0 && psectsz > 0)
		// 	ioctl(fd, DIOCGSTRIPEOFFSET, &psectoff);
		// strlcpy(arg.name, "GEOM::candelete", sizeof(arg.name));
		// arg.len = sizeof(arg.value.i);
		// if (ioctl(fd, DIOCGATTR, &arg) == 0)
		// 	candelete = arg.value.i;
		// if (ioctl(fd, DIOCGPROVIDERNAME, name) == 0)
		// 	geom = 1;
	} else
		psectsz = (size_t)sbuf.st_blksize;

	if (ssopt != 0) {
		if (!powerof2(ssopt) || !powerof2(pssopt) || ssopt < 512 ||
		    ssopt > pssopt) {
			fprintf(stderr, "Invalid sector size %d/%d\n",
			    ssopt, pssopt);
			goto err;
		}

		// /*
		//  * Some backend drivers (e.g. cd0, ada0) require that the I/O
		//  * size be a multiple of the device's sector size.
		//  *
		//  * Validate that the emulated sector size complies with this
		//  * requirement.
		//  */
		// if (S_ISCHR(sbuf.st_mode)) {
		// 	if (ssopt < sectsz || (ssopt % sectsz) != 0) {
		// 		fprintf(stderr, "Sector size %d incompatible "
		// 		    "with underlying device sector size %d\n",
		// 		    ssopt, sectsz);
		// 		goto err;
		// 	}
		// }

		sectsz = ssopt;
		psectsz = (size_t)pssopt;
		psectoff = 0;
	}

	if (sparse) {
		size_t len = strlen(nopt) + 6;
		char *filename = calloc(len, 1);
		snprintf(filename, len, "%s.lut", nopt);

		// open lut file
		sparse_fd = open(filename, (ro ? O_RDONLY : O_RDWR | O_CREAT) | extra);
		if (sparse_fd < 0 && !ro) {
			perror("Could not open sparse lut file r/w, reverting to readonly");
			/* Attempt a r/w fail with a r/o open */
			sparse_fd = open(filename, O_RDONLY | extra);
			ro = 1;
		}
		free(filename);

		if (sparse_fd < 0) {
			perror("Could not open sparse lut file");
			goto err;
		}

		if (fstat(sparse_fd, &sbuf) < 0) {
			perror("Could not stat sparse lut file");
			goto err;
		}

		if (sbuf.st_size == 0) {
			// TODO: make growing disks possible
			// create lut file
			printf(" -> sparse lut file does not exist, creating empty file\n");
			fchmod(sparse_fd, 0660);
			unsigned char buffer[4] = { 0xff, 0xff, 0xff, 0xff };
			for(size_t i = 0; i < size / (size_t)sectsz; i++) {
				write(sparse_fd, buffer, 4);
			}
			lseek(sparse_fd, 0, SEEK_SET);
			fstat(sparse_fd, &sbuf);
		}

		// read sparse lut
		sparse_lut = malloc((size_t)sbuf.st_size);
		ssize_t bytes = 0;
		for (size_t i = 0; i < (size_t)sbuf.st_size; i += 1024) {
			ssize_t result = read(sparse_fd, (char *)sparse_lut + i, 1024);
			if (result < 0) {
				perror("Could not load sparse lut");
				goto err;
			}
			bytes += result;
		}
		// fprintf(stderr, "Read %zu bytes of lut\n", bytes);
	}

	bc = calloc(1, sizeof(struct vdsk_split_ctx));
	if (bc == NULL) {
		perror("calloc");
		goto err;
	}

	if (split == 0) {
		bc->bc_num_fd = 1;
		bc->bc_fd = malloc(sizeof(int));
		bc->bc_fd[0] = fd;
	} else {
		bc->bc_num_fd = (int)(size / split);
		bc->bc_fd = fds;
	}
	bc->bc_sparse = sparse;
	if (sparse) {
		bc->bc_sparse_lut = sparse_lut;
		bc->bc_sparse_fd = sparse_fd;
	}
	bc->bc_ischr = S_ISCHR(sbuf.st_mode);
	bc->bc_split = split;
	bc->super.bc_isgeom = geom;
	bc->super.bc_candelete = candelete;
	bc->super.bc_rdonly = ro;
	bc->super.bc_size = (off_t)size;
	bc->super.bc_sectsz = sectsz;
	bc->super.bc_psectsz = (int) psectsz;
	bc->super.bc_psectoff = (int) psectoff;

	bc->super.close = disk_close;
	bc->super.read = disk_read;
	bc->super.write = disk_write;
	bc->super.flush = disk_flush;
	bc->super.delete = disk_delete;

	free(nopt);
	return (struct vdsk *)bc;
err:
	if (fd >= 0)
		close(fd);
	if (fds != NULL) {
		int i, num_fds = (int)(size / split);
		for (i = 0; i < num_fds; i++) {
			if (fds[i] >= 0) {
				close(fds[i]);
			}
		}
		free(fds);
	}
	free(nopt);
	return (NULL);
}
