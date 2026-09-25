package soteria

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import soteria.io.NioLocalFileSystem

class NioLocalFileSystemSuite extends SparkTestBase {

  private def perms(p: java.nio.file.Path) = PosixFilePermissions.toString(Files.getPosixFilePermissions(p))

  test("sessions use the in-process local filesystem") {
    val conf = session.spark.sparkContext.hadoopConfiguration
    assert(conf.get("fs.file.impl") == classOf[NioLocalFileSystem].getName)
    assert(FileSystem.getLocal(conf).getClass == classOf[NioLocalFileSystem])
  }

  test("sessions fetch shuffle blocks from their owner, not from local disk") {
    assert(session.spark.sparkContext.getConf.get("spark.shuffle.readHostLocalDisk") == "false")
  }

  test("setPermission and mkdirs apply POSIX permissions without child processes") {
    val fs = new NioLocalFileSystem
    fs.initialize(java.net.URI.create("file:///"), new Configuration())
    val dir = Files.createTempDirectory("soteria-nio")

    val file = dir.resolve("f")
    Files.write(file, Array[Byte](1))
    fs.setPermission(new Path(file.toUri), new FsPermission(Integer.parseInt("640", 8).toShort))
    assert(perms(file) == "rw-r-----")

    val nested = dir.resolve("a/b")
    assert(fs.mkdirs(new Path(nested.toUri), new FsPermission(Integer.parseInt("750", 8).toShort)))
    assert(perms(nested) == "rwxr-x---")

    fs.setOwner(new Path(file.toUri), null, null) // null means unchanged
    fs.setOwner(new Path(file.toUri), System.getProperty("user.name"), null)
  }
}
