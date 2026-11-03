package p08.pipeline;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

/** Pipelines selectable with "--pipeline <name>". */
public final class Pipelines {

    private static final Map<String, Supplier<Pipeline>> ALL = new TreeMap<>();

    static {
        ALL.put("page-window", PageWindow::new);
    }

    private Pipelines() {
    }

    public static Pipeline byName(String name) {
        Supplier<Pipeline> p = ALL.get(name);
        if (p == null) {
            throw new IllegalArgumentException("unknown pipeline " + name + ", known: " + ALL.keySet());
        }
        return p.get();
    }
}
