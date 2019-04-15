[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.vivimice/bgzf-randreader/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.vivimice/bgzf-randreader)

# Overview

**bgzf-randreader** is a BGZF reader supports random access relative to uncompressed data.

Suppose we have a BGZF file compressed from some text:

```sh
$ echo 'The quick brown fox jumps over the lazy dog' | bgzip > test.gz
```

We can random access any part of it via `RandomAccessBgzFile` without decompression:

```java
RandomAccessBgzFile file = new RandomAccessBgzFile(new File("test.gz"));
try {
    byte[] b = new byte[5];
    file.seek(4);
    file.read(b);
    System.out.println(new String(b));  // outputs: quick
} finally {
    file.close();   // always close it, prevent memory leak
}
```

# Maven dependencies

To use bgzf-randreader in Maven-based projects, use following dependency:

```xml
<dependency>
    <groupId>com.vivimice</groupId>
    <artifactId>bgzf-randreader</artifactId>
    <version>1.0.0</version>
</dependency>
```

# About BGZF

BGZF is a GZip compatible compression format. It is a block compression implemented on top of the standard gzip file format.

BGZF file can be generated from existing gzip file or any uncompressed data by `bgzip` utility. Any gzip compatible utility (like `gunzip`, `zcat`, `zgrep`, `GZIPInputStream`, etc.) can decompress BGZF compressed file.

On debian/Ubuntu, `bgzip` utility is included in `tabix` package.

More about BGZF: [http://samtools.github.io/hts-specs/SAMv1.pdf](http://samtools.github.io/hts-specs/SAMv1.pdf)
