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

import clojure.lang.IPersistentCollection;
import clojure.lang.ITransientCollection;
import clojure.lang.IFn;

import java.util.Iterator;

public final class LeveledValues {

  public static IPersistentCollection assemble(final IIntDecoder repetitionLevelsDecoder,
                                               final IIntDecoder definitionLevelsDecoder,
                                               final IDecoder dataDecoder,
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

  private static IPersistentCollection assembleRequired(final IDecoder dataDecoder) {
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    int i = 0;
    int n = dataDecoder.numEncodedValues();
    while (i < n) {
      vs.conj(dataDecoder.decode());
      i += 1;
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleRequiredFn(final IDecoder dataDecoder, final IFn fn) {
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    int i = 0;
    int n = dataDecoder.numEncodedValues();
    while (i < n) {
      vs.conj(fn.invoke(dataDecoder.decode()));
      i += 1;
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleNonRepeatedValue(final IIntDecoder definitionLevelsDecoder,
                                                                final IDecoder dataDecoder,
                                                                final int maxDefinitionLevel) {
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    int i = 0;
    int n = definitionLevelsDecoder.numEncodedValues();
    while (i < n) {
      if (definitionLevelsDecoder.decodeInt() == 0) {
        vs.conj(null);
      } else {
        vs.conj(dataDecoder.decode());
      }
      i += 1;
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleNonRepeatedValueFn(final IIntDecoder definitionLevelsDecoder,
                                                                  final IDecoder dataDecoder,
                                                                  final int maxDefinitionLevel,
                                                                  final IFn fn) {
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    int i = 0;
    int n = definitionLevelsDecoder.numEncodedValues();
    while (i < n) {
      if (definitionLevelsDecoder.decodeInt() == 0) {
        vs.conj(null);
      } else {
        vs.conj(fn.invoke(dataDecoder.decode()));
      }
      i += 1;
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleDefault(final IIntDecoder repetitionLevelsDecoder,
                                                       final IIntDecoder definitionLevelsDecoder,
                                                       final IDecoder dataDecoder,
                                                       final int maxDefinitionLevel) {
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    ITransientCollection rv = PersistentLinkedSeq.newEmptyTransient();
    boolean seenFirstValue = false;
    int i = 0;
    int n = repetitionLevelsDecoder.numEncodedValues();
    while (i < n) {
      int repLvl = repetitionLevelsDecoder.decodeInt();
      if (repLvl == 0 && seenFirstValue){
        vs.conj(rv.persistent());
        rv = PersistentLinkedSeq.newEmptyTransient();
      }
      seenFirstValue = true;
      int defLvl = definitionLevelsDecoder.decodeInt();
      if (defLvl < maxDefinitionLevel) {
        rv.conj(new LeveledValue(repLvl, defLvl, null));
      } else {
        rv.conj(new LeveledValue(repLvl, defLvl, dataDecoder.decode()));
      }
      i += 1;
    }
    if (rv != null) {
      vs.conj(rv.persistent());
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleDefaultFn(final IIntDecoder repetitionLevelsDecoder,
                                                         final IIntDecoder definitionLevelsDecoder,
                                                         final IDecoder dataDecoder,
                                                         final int maxDefinitionLevel,
                                                         final IFn fn) {
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    ITransientCollection rv = PersistentLinkedSeq.newEmptyTransient();
    boolean seenFirstValue = false;
    int i = 0;
    int n = repetitionLevelsDecoder.numEncodedValues();
    while (i < n) {
      int repLvl = repetitionLevelsDecoder.decodeInt();
      if (repLvl == 0 && seenFirstValue){
        vs.conj(rv.persistent());
        rv = PersistentLinkedSeq.newEmptyTransient();
      }
      seenFirstValue = true;
      int defLvl = definitionLevelsDecoder.decodeInt();
      if (defLvl < maxDefinitionLevel) {
        rv.conj(new LeveledValue(repLvl, defLvl, null));
      } else {
        rv.conj(new LeveledValue(repLvl, defLvl, fn.invoke(dataDecoder.decode())));
      }
      i += 1;
    }
    if (rv != null) {
      vs.conj(rv.persistent());
    }
    return vs.persistent();
  }


}
