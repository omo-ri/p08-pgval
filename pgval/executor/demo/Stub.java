import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public final class Stub {
    // Version 1 uses compact JSON with fixed field order and ASCII event IDs.
    private static final Pattern EVENT = Pattern.compile(
        "\\{\"type\":\"event\",\"event_id\":\"([A-Za-z0-9_-]{1,64})\",\"value\":(-?(?:0|[1-9][0-9]*))\\}");

    public static void main(String[] args) throws Exception {
        var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        var output = new PrintWriter(System.out, true, StandardCharsets.UTF_8);
        output.println("{\"type\":\"ready\"}");
        for (String line; (line = input.readLine()) != null;) {
            if (line.equals("{\"type\":\"finish\"}")) {
                return;
            }
            var event = EVENT.matcher(line);
            if (!event.matches()) {
                throw new IllegalArgumentException("expected a v1 event or finish message");
            }
            String id = event.group(1);
            long value = Integer.parseInt(event.group(2));
            if (id.equals("e2")) {
                output.println("{\"type\":\"stage\",\"name\":\"before_result\",\"event_id\":\"e2\"}");
                if (!"{\"type\":\"continue\",\"event_id\":\"e2\"}".equals(input.readLine())) {
                    throw new IllegalStateException("barrier requires continue(e2)");
                }
            }
            output.println("{\"type\":\"result\",\"event_id\":\"" + id + "\",\"result\":" + (2 * value) + "}");
        }
        throw new IllegalStateException("stdin closed before finish");
    }
}
