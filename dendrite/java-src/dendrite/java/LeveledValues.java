/**
* Copyright (c) 2013-2014 John Whitbeck. All rights reserved.
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

import clojure.lang.IPersistentCollection;
import clojure.lang.ITransientCollection;
import clojure.lang.IFn;

import java.util.Iterator;

public final class LeveledValues {

  public static IPersistentCollection assemble(final IntDecoder repetitionLevelsDecoder,
                                               final IntDecoder definitionLevelsDecoder,
                                               final Decoder dataDecoder,
                                               final int maxDefinitionLevel,
                                               final IFn fn) {
    if (repetitionLevelsDecoder == null) {
      if (definitionLevelsDecoder == null) {
        if (fn == null) {
          return assembleRequired(dataDecoder);
        }
        return assembleRequiredFn(dataDecoder, fn);
      }
      if (fn == null) {
        return assembleNonRepeatedValue(definitionLevelsDecoder, dataDecoder, maxDefinitionLevel);
      }
      return assembleNonRepeatedValueFn(definitionLevelsDecoder, dataDecoder, maxDefinitionLevel, fn);
    }
    if (fn == null) {
      return assembleDefault(repetitionLevelsDecoder, definitionLevelsDecoder, dataDecoder,
                             maxDefinitionLevel);
    }
    return assembleDefaultFn(repetitionLevelsDecoder, definitionLevelsDecoder, dataDecoder,
                             maxDefinitionLevel, fn);
  }

  private static IPersistentCollection assembleRequired(final Decoder dataDecoder) {
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    for (Object o : dataDecoder) {
      vs.conj(new Singleton(new LeveledValue(0, 0, o)));
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleRequiredFn(final Decoder dataDecoder, final IFn fn) {
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    for (Object o : dataDecoder) {
      vs.conj(new Singleton(new LeveledValue(0, 0, fn.invoke(o))));
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleNonRepeatedValue(final IntDecoder definitionLevelsDecoder,
                                                                final Decoder dataDecoder,
                                                                final int maxDefinitionLevel) {
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    final IntIterator defLvlIterator = definitionLevelsDecoder.intIterator();
    final Iterator dataIterator = dataDecoder.iterator();
    while (defLvlIterator.hasNext()){
      int defLvl = defLvlIterator.next();
      if (defLvl < maxDefinitionLevel) {
        vs.conj(new Singleton(new LeveledValue(0, defLvl, null)));
      } else {
        vs.conj(new Singleton(new LeveledValue(0, defLvl, dataIterator.next())));
      }
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleNonRepeatedValueFn(final IntDecoder definitionLevelsDecoder,
                                                                  final Decoder dataDecoder,
                                                                  final int maxDefinitionLevel,
                                                                  final IFn fn) {
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    final IntIterator defLvlIterator = definitionLevelsDecoder.intIterator();
    final Iterator dataIterator = dataDecoder.iterator();
    while (defLvlIterator.hasNext()){
      int defLvl = defLvlIterator.next();
      if (defLvl < maxDefinitionLevel) {
        vs.conj(new Singleton(new LeveledValue(0, defLvl, null)));
      } else {
        vs.conj(new Singleton(new LeveledValue(0, defLvl, fn.invoke(dataIterator.next()))));
      }
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleDefault(final IntDecoder repetitionLevelsDecoder,
                                                       final IntDecoder definitionLevelsDecoder,
                                                       final Decoder dataDecoder,
                                                       final int maxDefinitionLevel) {
    final IntIterator defLvlIterator = definitionLevelsDecoder.intIterator();
    final IntIterator repLvlIterator = repetitionLevelsDecoder.intIterator();
    final Iterator dataIterator = dataDecoder.iterator();
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    ITransientCollection rv = PersistentLinkedSeq.newEmptyTransient();
    boolean seenFirstValue = false;
    while (repLvlIterator.hasNext()){
      int repLvl = repLvlIterator.next();
      if (repLvl == 0 && seenFirstValue){
        vs.conj(rv.persistent());
        rv = PersistentLinkedSeq.newEmptyTransient();
      }
      seenFirstValue = true;
      int defLvl = defLvlIterator.next();
      if (defLvl < maxDefinitionLevel) {
        rv.conj(new LeveledValue(repLvl, defLvl, null));
      } else {
        rv.conj(new LeveledValue(repLvl, defLvl, dataIterator.next()));
      }
    }
    if (rv != null) {
      vs.conj(rv.persistent());
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleDefaultFn(final IntDecoder repetitionLevelsDecoder,
                                                         final IntDecoder definitionLevelsDecoder,
                                                         final Decoder dataDecoder,
                                                         final int maxDefinitionLevel,
                                                         final IFn fn) {
    final IntIterator defLvlIterator = definitionLevelsDecoder.intIterator();
    final IntIterator repLvlIterator = repetitionLevelsDecoder.intIterator();
    final Iterator dataIterator = dataDecoder.iterator();
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    ITransientCollection rv = PersistentLinkedSeq.newEmptyTransient();
    boolean seenFirstValue = false;
    while (repLvlIterator.hasNext()){
      int repLvl = repLvlIterator.next();
      if (repLvl == 0 && seenFirstValue){
        vs.conj(rv.persistent());
        rv = PersistentLinkedSeq.newEmptyTransient();
      }
      seenFirstValue = true;
      int defLvl = defLvlIterator.next();
      if (defLvl < maxDefinitionLevel) {
        rv.conj(new LeveledValue(repLvl, defLvl, null));
      } else {
        rv.conj(new LeveledValue(repLvl, defLvl, fn.invoke(dataIterator.next())));
      }
    }
    if (rv != null) {
      vs.conj(rv.persistent());
    }
    return vs.persistent();
  }


}
