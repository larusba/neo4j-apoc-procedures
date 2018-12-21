package apoc.util;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.HashMap;
import java.util.Map;

import static apoc.util.Util.getFormat;

public class DateParseUtil {

    public static Map<Class<? extends TemporalAccessor>, MethodHandle> parseDateMap = new HashMap<>();
    public static Map<Class<? extends TemporalAccessor>, MethodHandle> simpleParseDateMap = new HashMap<>();
    public static String METHOD_NAME = "parse";

    public static TemporalAccessor dateParse(String value, Class<? extends TemporalAccessor> date, String...formats) {
        try {
            if (formats != null && formats.length > 0) {
                for (String form : formats) {
                    try {
                        try {
                            return getParse(date, getFormat(form), value);
                        } catch (DateTimeParseException e) {
                            return getParse(date, value);
                        }
                    } catch (Exception e) {
                        continue;
                    }
                }
            } else {
                return getParse(date, value);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        throw new RuntimeException("Can't format the date with the pattern");
    }

    private static TemporalAccessor getParse(Class<? extends TemporalAccessor> date, DateTimeFormatter format, String value) throws Throwable {
        MethodHandle methodHandle;

        if (parseDateMap.containsKey(date)) {
            methodHandle = parseDateMap.get(date);
        } else {
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();
            methodHandle = lookup.findStatic(date, METHOD_NAME, MethodType.methodType(date, CharSequence.class, DateTimeFormatter.class));
            parseDateMap.put(date, methodHandle);
        }
        return (TemporalAccessor) methodHandle.invokeWithArguments(value, format);
    }

    private static TemporalAccessor getParse(Class<? extends TemporalAccessor> date, String value) throws Throwable {
        MethodHandle methodHandleSimple;

        if (simpleParseDateMap.containsKey(date)) {
            methodHandleSimple = simpleParseDateMap.get(date);
        } else {
            MethodHandles.Lookup lookup = MethodHandles.publicLookup();
            methodHandleSimple = lookup.findStatic(date, METHOD_NAME, MethodType.methodType(date, CharSequence.class));
            simpleParseDateMap.put(date, methodHandleSimple);
        }
        return (TemporalAccessor) methodHandleSimple.invokeWithArguments(value);
    }

}
