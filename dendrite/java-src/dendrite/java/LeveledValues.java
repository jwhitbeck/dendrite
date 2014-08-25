package dendrite.java;

import clojure.lang.IPersistentCollection;
import clojure.lang.ITransientCollection;
import clojure.lang.IFn;

import java.util.Iterator;

public final class LeveledValues {

  public static IPersistentCollection assemble(final Decoder repetitionLevelsDecoder,
                                               final Decoder definitionLevelsDecoder,
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

  private static IPersistentCollection assembleNonRepeatedValue(final Decoder definitionLevelsDecoder,
                                                                final Decoder dataDecoder,
                                                                final int maxDefinitionLevel) {
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    final Iterator defLvlIterator = definitionLevelsDecoder.iterator();
    final Iterator dataIterator = dataDecoder.iterator();
    while (defLvlIterator.hasNext()){
      int defLvl = (int) defLvlIterator.next();
      if (defLvl < maxDefinitionLevel) {
        vs.conj(new Singleton(new LeveledValue(0, defLvl, null)));
      } else {
        vs.conj(new Singleton(new LeveledValue(0, defLvl, dataIterator.next())));
      }
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleNonRepeatedValueFn(final Decoder definitionLevelsDecoder,
                                                                  final Decoder dataDecoder,
                                                                  final int maxDefinitionLevel,
                                                                  final IFn fn) {
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    final Iterator defLvlIterator = definitionLevelsDecoder.iterator();
    final Iterator dataIterator = dataDecoder.iterator();
    while (defLvlIterator.hasNext()){
      int defLvl = (int) defLvlIterator.next();
      if (defLvl < maxDefinitionLevel) {
        vs.conj(new Singleton(new LeveledValue(0, defLvl, null)));
      } else {
        vs.conj(new Singleton(new LeveledValue(0, defLvl, fn.invoke(dataIterator.next()))));
      }
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleDefault(final Decoder repetitionLevelsDecoder,
                                                       final Decoder definitionLevelsDecoder,
                                                       final Decoder dataDecoder,
                                                       final int maxDefinitionLevel) {
    final Iterator defLvlIterator = definitionLevelsDecoder.iterator();
    final Iterator repLvlIterator = repetitionLevelsDecoder.iterator();
    final Iterator dataIterator = dataDecoder.iterator();
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    ITransientCollection rv = PersistentLinkedSeq.newEmptyTransient();
    boolean seenFirstValue = false;
    while (repLvlIterator.hasNext()){
      int repLvl = (int)repLvlIterator.next();
      if (repLvl == 0 && seenFirstValue){
        vs.conj(rv.persistent());
        rv = PersistentLinkedSeq.newEmptyTransient();
      }
      seenFirstValue = true;
      int defLvl = (int)defLvlIterator.next();
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

  private static IPersistentCollection assembleDefaultFn(final Decoder repetitionLevelsDecoder,
                                                         final Decoder definitionLevelsDecoder,
                                                         final Decoder dataDecoder,
                                                         final int maxDefinitionLevel,
                                                         final IFn fn) {
    final Iterator defLvlIterator = definitionLevelsDecoder.iterator();
    final Iterator repLvlIterator = repetitionLevelsDecoder.iterator();
    final Iterator dataIterator = dataDecoder.iterator();
    ITransientCollection vs = PersistentLinkedSeq.newEmptyTransient();
    ITransientCollection rv = PersistentLinkedSeq.newEmptyTransient();
    boolean seenFirstValue = false;
    while (repLvlIterator.hasNext()){
      int repLvl = (int)repLvlIterator.next();
      if (repLvl == 0 && seenFirstValue){
        vs.conj(rv.persistent());
        rv = PersistentLinkedSeq.newEmptyTransient();
      }
      seenFirstValue = true;
      int defLvl = (int)defLvlIterator.next();
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
