package soteria.io

import java.nio.file.{Files, LinkOption}
import java.nio.file.attribute.{PosixFileAttributeView, PosixFilePermission}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.{LocalFileSystem, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.FsPermission

/**
 * `file://` filesystem that changes permissions and owners with Java NIO.
 *
 * Without the native Hadoop library (not shipped with Spark), Hadoop's
 * RawLocalFileSystem runs `chmod`/`chown` in a child process, e.g. for every
 * directory an output committer creates. Inside Gramine, starting a child
 * process from a JVM fails, so writing any output failed. This class does the
 * same changes in-process.
 */
class NioRawLocalFileSystem extends RawLocalFileSystem {

  override def setPermission(p: Path, permission: FsPermission): Unit = {
    val mode = permission.toShort & 0x1ff // rwxrwxrwx; setuid/setgid/sticky are not set
    val bits = Seq(
      PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE,
      PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE,
      PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE)
    val perms = bits.zipWithIndex.collect { case (perm, i) if (mode & (1 << (8 - i))) != 0 => perm }
    Files.setPosixFilePermissions(pathToFile(p).toPath, perms.toSet.asJava)
  }

  /** A null `username` or `groupname` leaves that attribute unchanged, as in Hadoop. */
  override def setOwner(p: Path, username: String, groupname: String): Unit = {
    val path = pathToFile(p).toPath
    val view = Files.getFileAttributeView(path, classOf[PosixFileAttributeView], LinkOption.NOFOLLOW_LINKS)
    val lookup = path.getFileSystem.getUserPrincipalLookupService
    if (username != null) view.setOwner(lookup.lookupPrincipalByName(username))
    if (groupname != null) view.setGroup(lookup.lookupPrincipalByGroupName(groupname))
  }
}

/** Checksummed local filesystem (like the default `file://`) over [[NioRawLocalFileSystem]]. */
class NioLocalFileSystem extends LocalFileSystem(new NioRawLocalFileSystem)
