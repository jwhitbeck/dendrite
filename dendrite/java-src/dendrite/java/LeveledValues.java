package dendrite.java;

import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;
import clojure.lang.ITransientVector;
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
    ITransientVector vs = PersistentVector.EMPTY.asTransient();
    for (Object o : dataDecoder) {
      vs.conj(new Singleton(new LeveledValue(0, 0, o)));
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleRequiredFn(final Decoder dataDecoder, final IFn fn) {
    ITransientVector vs = PersistentVector.EMPTY.asTransient();
    for (Object o : dataDecoder) {
      vs.conj(new Singleton(new LeveledValue(0, 0, fn.invoke(o))));
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleNonRepeatedValue(final Decoder definitionLevelsDecoder,
                                                                final Decoder dataDecoder,
                                                                final int maxDefinitionLevel) {
    ITransientVector vs = PersistentVector.EMPTY.asTransient();
    final Iterator def_lvl_i = definitionLevelsDecoder.iterator();
    final Iterator data_i = dataDecoder.iterator();
    while (def_lvl_i.hasNext()){
      int def_lvl = (int) def_lvl_i.next();
      if (def_lvl < maxDefinitionLevel) {
        vs.conj(new Singleton(new LeveledValue(0, def_lvl, null)));
      } else {
        vs.conj(new Singleton(new LeveledValue(0, def_lvl, data_i.next())));
      }
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleNonRepeatedValueFn(final Decoder definitionLevelsDecoder,
                                                                  final Decoder dataDecoder,
                                                                  final int maxDefinitionLevel,
                                                                  final IFn fn) {
    ITransientVector vs = PersistentVector.EMPTY.asTransient();
    final Iterator def_lvl_i = definitionLevelsDecoder.iterator();
    final Iterator data_i = dataDecoder.iterator();
    while (def_lvl_i.hasNext()){
      int def_lvl = (int) def_lvl_i.next();
      if (def_lvl < maxDefinitionLevel) {
        vs.conj(new Singleton(new LeveledValue(0, def_lvl, null)));
      } else {
        vs.conj(new Singleton(new LeveledValue(0, def_lvl, fn.invoke(data_i.next()))));
      }
    }
    return vs.persistent();
  }

  private static IPersistentCollection assembleDefault(final Decoder repetitionLevelsDecoder,
                                                       final Decoder definitionLevelsDecoder,
                                                       final Decoder dataDecoder,
                                                       final int maxDefinitionLevel) {
    final Iterator def_lvl_i = definitionLevelsDecoder.iterator();
    final Iterator rep_lvl_i = repetitionLevelsDecoder.iterator();
    final Iterator data_i = dataDecoder.iterator();
    ITransientVector vs = PersistentVector.EMPTY.asTransient();
    ITransientVector rv = PersistentVector.EMPTY.asTransient();
    boolean seen_first_value = false;
    while (rep_lvl_i.hasNext()){
      int rep_lvl = (int)rep_lvl_i.next();
      if (rep_lvl == 0 && seen_first_value){
        vs.conj(rv.persistent());
        rv = PersistentVector.EMPTY.asTransient();
      }
      seen_first_value = true;
      int def_lvl = (int)def_lvl_i.next();
      if (def_lvl < maxDefinitionLevel) {
        rv.conj(new LeveledValue(rep_lvl, def_lvl, null));
      } else {
        rv.conj(new LeveledValue(rep_lvl, def_lvl, data_i.next()));
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
    final Iterator def_lvl_i = definitionLevelsDecoder.iterator();
    final Iterator rep_lvl_i = repetitionLevelsDecoder.iterator();
    final Iterator data_i = dataDecoder.iterator();
    ITransientVector vs = PersistentVector.EMPTY.asTransient();
    ITransientVector rv = PersistentVector.EMPTY.asTransient();
    boolean seen_first_value = false;
    while (rep_lvl_i.hasNext()){
      int rep_lvl = (int)rep_lvl_i.next();
      if (rep_lvl == 0 && seen_first_value){
        vs.conj(rv.persistent());
        rv = PersistentVector.EMPTY.asTransient();
      }
      seen_first_value = true;
      int def_lvl = (int)def_lvl_i.next();
      if (def_lvl < maxDefinitionLevel) {
        rv.conj(new LeveledValue(rep_lvl, def_lvl, null));
      } else {
        rv.conj(new LeveledValue(rep_lvl, def_lvl, fn.invoke(data_i.next())));
      }
    }
    if (rv != null) {
      vs.conj(rv.persistent());
    }
    return vs.persistent();
  }


}
