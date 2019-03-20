package io.github.risdenk.hadoop.s3a;

import cloud.localstack.DockerTestUtils;
import cloud.localstack.docker.LocalstackDocker;
import cloud.localstack.docker.LocalstackDockerTestRunner;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.IOUtils;
import com.amazonaws.util.StringInputStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.Locale;

import static org.junit.Assert.*;

@RunWith(LocalstackDockerTestRunner.class)
@LocalstackDockerProperties(randomizePorts = true, services = { "s3" })
public class TestS3ALocalstack {
  private String bucketName;
  private String key;
  private String key2;
  private String key3;
  private String val;
  private String val2;

  private AmazonS3 s3Client;

  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    bucketName = RandomStringUtils.randomAlphabetic(5).toLowerCase(Locale.ROOT);
    key = RandomStringUtils.randomAlphabetic(5);
    key2 = RandomStringUtils.randomAlphabetic(5);
    key3 = RandomStringUtils.randomAlphabetic(5);
    val = RandomStringUtils.randomAlphabetic(1000000);
    val2 = RandomStringUtils.randomAlphabetic(1000000);

    s3Client = DockerTestUtils.getClientS3();
    assertNotNull(s3Client.createBucket(bucketName));
    assertEquals(1, s3Client.listBuckets().size());

    assertNotNull(s3Client.putObject(bucketName, key, val));
    try(S3Object s3Object = s3Client.getObject(bucketName, key)) {
      try (S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent()) {
        assertEquals(val, IOUtils.toString(s3ObjectInputStream));
      }
    }

    String s3Endpoint = LocalstackDocker.INSTANCE.getEndpointS3();
    conf = new Configuration();
    conf.setBoolean("fs.s3a.impl.disable.cache", true);
    conf.set("fs.s3a.endpoint", s3Endpoint);
    conf.setBoolean("fs.s3a.path.style.access", true);
  }

  @After
  public void tearDown() {
    emptyAndDeleteBucket(bucketName);
  }

  // https://docs.aws.amazon.com/AmazonS3/latest/dev/delete-or-empty-bucket.html#delete-bucket-sdk-java
  private void emptyAndDeleteBucket(String bucketName) {
    // Delete all objects from the bucket. This is sufficient for unversioned buckets.
    ObjectListing objectListing = s3Client.listObjects(bucketName);
    while (true) {
      for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
        s3Client.deleteObject(bucketName, s3ObjectSummary.getKey());
      }

      // If the bucket contains many objects, the listObjects() call
      // might not return all of the objects in the first listing. Check to
      // see whether the listing was truncated. If so, retrieve the next page of objects
      // and delete them.
      if (objectListing.isTruncated()) {
        objectListing = s3Client.listNextBatchOfObjects(objectListing);
      } else {
        break;
      }
    }

    // After all objects and object versions are deleted, delete the bucket.
    s3Client.deleteBucket(bucketName);
  }

  @Test
  public void testS3ALocalStackFileSystem() throws Exception {
    URI uri = new URI("s3a://" + bucketName + '/');
    try(FileSystem fs = FileSystem.get(uri, conf)) {
      RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(uri), true);
      assertTrue(listFiles.hasNext());
      LocatedFileStatus fileStatus = listFiles.next();
      Path filePath = fileStatus.getPath();
      assertEquals(uri.toString() + key, filePath.toString());
      try(FSDataInputStream fsDataInputStream = fs.open(filePath)) {
        assertEquals(val, IOUtils.toString(fsDataInputStream));
      }

      Path filePath2 = new Path(uri + key2);
      try(FSDataOutputStream fsDataOutputStream = fs.create(filePath2)) {
        IOUtils.copy(new StringInputStream(val2), fsDataOutputStream);
      }

      try(FSDataInputStream fsDataInputStream = fs.open(filePath2)) {
        assertEquals(val2, IOUtils.toString(fsDataInputStream));
      }

      try(S3Object s3Object = s3Client.getObject(bucketName, key2)) {
        try (S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent()) {
          assertEquals(val2, IOUtils.toString(s3ObjectInputStream));
        }
      }

      Path filePath3 = new Path(uri + key3);
      assertTrue(fs.rename(filePath2, filePath3));

      try(FSDataInputStream fsDataInputStream = fs.open(filePath3)) {
        assertEquals(val2, IOUtils.toString(fsDataInputStream));
      }

      try(S3Object s3Object = s3Client.getObject(bucketName, key3)) {
        try (S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent()) {
          assertEquals(val2, IOUtils.toString(s3ObjectInputStream));
        }
      }
    }
  }

  @Test
  public void testS3ALocalStackFileContext() throws Exception {
    URI uri = new URI("s3a://" + bucketName + '/');
    FileContext fileContext = FileContext.getFileContext(uri, conf);
    RemoteIterator<FileStatus> listFiles = fileContext.listStatus(new Path(uri));
    assertTrue(listFiles.hasNext());
    FileStatus fileStatus = listFiles.next();
    Path filePath = fileStatus.getPath();
    assertEquals(uri.toString() + key, filePath.toString());
    try(FSDataInputStream fsDataInputStream = fileContext.open(filePath)) {
      assertEquals(val, IOUtils.toString(fsDataInputStream));
    }

    Path filePath2 = new Path(uri + key2);
    try(FSDataOutputStream fsDataOutputStream = fileContext.create(filePath2).build()) {
      IOUtils.copy(new StringInputStream(val2), fsDataOutputStream);
    }

    try(FSDataInputStream fsDataInputStream = fileContext.open(filePath2)) {
      assertEquals(val2, IOUtils.toString(fsDataInputStream));
    }

    try(S3Object s3Object = s3Client.getObject(bucketName, key2)) {
      try (S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent()) {
        assertEquals(val2, IOUtils.toString(s3ObjectInputStream));
      }
    }

    Path filePath3 = new Path(uri + key3);
    fileContext.rename(filePath2, filePath3);

    try(FSDataInputStream fsDataInputStream = fileContext.open(filePath3)) {
      assertEquals(val2, IOUtils.toString(fsDataInputStream));
    }

    try(S3Object s3Object = s3Client.getObject(bucketName, key3)) {
      try (S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent()) {
        assertEquals(val2, IOUtils.toString(s3ObjectInputStream));
      }
    }
  }
}
