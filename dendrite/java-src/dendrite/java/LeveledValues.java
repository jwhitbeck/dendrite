package dendrite.java;

import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentVector;
import clojure.lang.PersistentVector;
import clojure.lang.ITransientVector;

import java.util.Iterator;

public final class LeveledValues {

  public static IPersistentCollection assemble(final Decoder repetitionLevelsDecoder,
                                           final Decoder definitionLevelsDecoder,
                                           final Decoder dataDecoder,
                                           final int maxDefinitionLevel) {
    if (repetitionLevelsDecoder == null) {
      if (definitionLevelsDecoder == null) {
        return assembleRequired(dataDecoder);
      }
      return assembleNonRepeatedValue(definitionLevelsDecoder, dataDecoder, maxDefinitionLevel);
    }
    return assembleDefault(repetitionLevelsDecoder, definitionLevelsDecoder, dataDecoder, maxDefinitionLevel);
  }

  private static IPersistentCollection assembleRequired(final Decoder dataDecoder) {
    ITransientVector vs = PersistentVector.EMPTY.asTransient();
    for (Object o : dataDecoder) {
      vs.conj(new LeveledValue(0, 0, o));
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
      int def_lvl = def_lvl_i != null? (int)def_lvl_i.next() : 0;
      if (def_lvl < maxDefinitionLevel) {
        vs.conj(new LeveledValue(0, def_lvl, null));
      } else {
        vs.conj(new LeveledValue(0, def_lvl, data_i.next()));
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
    while (def_lvl_i.hasNext()){
      int def_lvl = (int)def_lvl_i.next();
      int rep_lvl = (int)rep_lvl_i.next();
      if (def_lvl < maxDefinitionLevel) {
        vs.conj(new LeveledValue(rep_lvl, def_lvl, null));
      } else {
        vs.conj(new LeveledValue(rep_lvl, def_lvl, data_i.next()));
      }
    }
    return vs.persistent();
  }

}
