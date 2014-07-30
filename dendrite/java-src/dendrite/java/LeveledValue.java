package dendrite.java;

import clojure.lang.IFn;

public final class LeveledValue {
  final public int repetitionLevel;
  final public int definitionLevel;
  final public Object value;

  public LeveledValue(final int replvl, final int deflvl, final Object v) {
    repetitionLevel = replvl;
    definitionLevel = deflvl;
    value = v;
  }

  public LeveledValue apply(final IFn fn){
    return new LeveledValue(repetitionLevel, definitionLevel, fn.invoke(value));
  }

  public LeveledValue assoc(final Object v){
    return new LeveledValue(repetitionLevel, definitionLevel, v);
  }

  @Override
  public boolean equals(final Object o){
    if (o instanceof LeveledValue) {
      LeveledValue lv = (LeveledValue)o;
      return lv.repetitionLevel == repetitionLevel && lv.definitionLevel == definitionLevel
        && ((value == null && lv.value == null)
            || (value != null && lv.value !=null && lv.value.equals(value)));
    }
    return false;
  }
}
