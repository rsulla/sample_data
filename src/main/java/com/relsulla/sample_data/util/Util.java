package com.relsulla.sample_data.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by Bob on 2/9/2017.
 */
public class Util {
    public static FileSystem getFileSystem(String path
                                          , Configuration conf) throws Exception {

        return(getFileSystem(new Path(path),conf));

    }

    public static FileSystem getFileSystem(Path path
                                          ,Configuration conf) throws Exception {

        return(FileSystem.get(path.toUri(), conf));

    }

    public static void createDirIfNecessary(FileSystem fs
                                           ,String path
                                           ,boolean parentFl) throws Exception {

        createDirIfNecessary(fs,new Path(path),parentFl);

    }

    public static void createDirIfNecessary(FileSystem fs
                                           ,Path path
                                           ,boolean parentFl) throws Exception {

        String[] parts = path.toString().split(Path.SEPARATOR);
        String currPath = "";
        Path createPath;
        int max = parts.length;

        if ( parentFl ) {
            max--;
        }

        for ( int idx = 0; idx < max; idx++ ) {
            if ( idx > 0 ) {
                currPath+= Path.SEPARATOR;
            }
            currPath+= parts[idx];

            if ( !(parts[idx].toLowerCase().startsWith("hdfs") || parts[idx].toLowerCase().startsWith("s3") || parts[idx].trim().length() == 0) ) {
                createPath = new Path(currPath);

                if ( !fs.exists(createPath) ) {
                    fs.mkdirs(createPath);
                }
            }
        }

    }

    public static void removeDirIfExists(FileSystem fs
                                        ,String path) throws Exception {

        removeDirIfExists(fs,new Path(path));

    }

    public static void removeDirIfExists(FileSystem fs
                                        ,Path path) throws Exception {

        if ( fs.exists(path) ) {
            fs.delete(path,true);
        }

    }


}
