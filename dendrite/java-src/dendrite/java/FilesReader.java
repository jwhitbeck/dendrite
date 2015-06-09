/**
 * Copyright (c) 2013-2015 John Whitbeck. All rights reserved.
 *
 * The use and distribution terms for this software are covered by the
 * Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 * which can be found in the file epl-v10.txt at the root of this distribution.
 * By using this software in any fashion, you are agreeing to be bound by
 * the terms of this license.
 *
 * You must not remove this notice, or any other, from this software.
 */

package dendrite.java;

import clojure.lang.IChunk;
import clojure.lang.IFn;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public final class FilesReader implements Closeable, IReader {

  private final List<File> files;
  private final Options.ReaderOptions readerOptions;
  private final Set<FileReader> openFileReaders;

  public FilesReader(Options.ReaderOptions readerOptions, List<File> files) {
    this.readerOptions = readerOptions;
    this.files = files;
    this.openFileReaders = Collections.synchronizedSet(new HashSet<FileReader>());
  }

  private synchronized FileReader openFileReader(File file) {
    if (openFileReaders == null) {
      throw new IllegalStateException("FilesReader attempted to open new file but it is already closed");
    }
    try {
      FileReader fileReader = FileReader.create(readerOptions, file);
      openFileReaders.add(fileReader);
      return fileReader;
    } catch (IOException e) {
      throw new IllegalStateException("Error while opening file "+file, e);
    }
  }

  private synchronized void closeFileReader(FileReader fileReader) {
    try {
      fileReader.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    } finally {
      openFileReaders.remove(fileReader);
    }
  }

  @Override
  public View read(Options.ReadOptions readOptions) {
    return new LazyView(readOptions);
  }

  @Override
  public synchronized void close() throws IOException {
    if (openFileReaders != null) {
      List<FileReader> fileReaderstoClose = new ArrayList<FileReader>(openFileReaders);
      for (FileReader fileReader : fileReaderstoClose) {
        closeFileReader(fileReader);
      }
    }
  }

  private Iterator<IChunk> getRecordChunks(final Options.ReadOptions readOptions, final int bundleSize) {
    final Iterator<File> fileIterator = files.iterator();
    if (!fileIterator.hasNext()) {
      return Collections.<IChunk>emptyList().iterator();
    }
    return new AReadOnlyIterator<IChunk>() {
      private FileReader currentFileReader = openFileReader(fileIterator.next());
      private Iterator<IChunk> currentViewChunks
        = currentFileReader.read(readOptions).getRecordChunks(bundleSize).iterator();

      private void step() {
        closeFileReader(currentFileReader);
        currentFileReader = openFileReader(fileIterator.next());
        currentViewChunks = currentFileReader.read(readOptions).getRecordChunks(bundleSize).iterator();
      }

      @Override
      public boolean hasNext() {
        if (currentViewChunks.hasNext()) {
          return true;
        } else if (fileIterator.hasNext()) {
          step();
          return hasNext();
        } else {
          return false;
        }
      }

      @Override
      public IChunk next() {
        if (currentViewChunks.hasNext()) {
          return currentViewChunks.next();
        } else if (fileIterator.hasNext()) {
          step();
          return next();
        } else {
          throw new NoSuchElementException();
        }
      }

    };
  }

  private Iterator<Object> getReducedChunkValues(final Options.ReadOptions readOptions,
                                                 final IFn f,
                                                 final Object init,
                                                 final int bundleSize) {
    final Iterator<File> fileIterator = files.iterator();
    if (!fileIterator.hasNext()) {
      return Collections.<Object>emptyList().iterator();
    }
    return new AReadOnlyIterator<Object>() {
      private FileReader currentFileReader = openFileReader(fileIterator.next());
      private Iterator<Object> currentReducedChunkValues
        = currentFileReader.read(readOptions).getReducedChunkValues(f, init, bundleSize).iterator();

      private void step() {
        closeFileReader(currentFileReader);
        currentFileReader = openFileReader(fileIterator.next());
        currentReducedChunkValues
          = currentFileReader.read(readOptions).getReducedChunkValues(f, init, bundleSize).iterator();
      }

      @Override
      public boolean hasNext() {
        if (currentReducedChunkValues.hasNext()) {
          return true;
        } else if (fileIterator.hasNext()) {
          step();
          return hasNext();
        } else {
          return false;
        }
      }

      @Override
      public Object next() {
        if (currentReducedChunkValues.hasNext()) {
          return currentReducedChunkValues.next();
        } else if (fileIterator.hasNext()) {
          step();
          return next();
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  private class LazyView extends View {

    private final Options.ReadOptions readOptions;

    LazyView(Options.ReadOptions readOptions) {
      super(readOptions.bundleSize);
      this.readOptions = readOptions;
    }

    @Override
    protected Options.ReadOptions getReadOptions() {
      return readOptions;
    }

    @Override
    protected View withOptions(Options.ReadOptions readOptions) {
      return new LazyView(readOptions);
    }

    @Override
    protected Iterable<IChunk> getRecordChunks(final int bundleSize) {
      return new Iterable<IChunk>() {
        @Override
        public Iterator<IChunk> iterator() {
          return FilesReader.this.getRecordChunks(readOptions, bundleSize);
        }
      };
    }

    @Override
    protected Iterable<Object> getReducedChunkValues(final IFn f, final Object init, final int bundleSize) {
      return new Iterable<Object>() {
        @Override
        public Iterator<Object> iterator() {
          return FilesReader.this.getReducedChunkValues(readOptions, f, init, bundleSize);
        }
      };
    }

  }
}
