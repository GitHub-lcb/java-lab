package lab.jvm;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lab.jvm.samples.ArithmeticSample;
import lab.redis.Json;

public final class JvmProbeService {
    private static volatile Object allocationSink;

    public String probe(String name) throws Exception {
        if ("runtime".equals(name)) return response(name, runtime());
        if ("bytecode".equals(name)) return response(name, bytecode());
        if ("classloaders".equals(name)) return response(name, classloaders());
        if ("memory".equals(name)) return response(name, memory());
        if ("allocation".equals(name)) return response(name, allocation());
        if ("references".equals(name)) return response(name, references());
        if ("gc".equals(name)) return response(name, garbageCollectors());
        throw new IllegalArgumentException("Unknown JVM probe");
    }

    private String runtime() {
        Runtime runtime = Runtime.getRuntime();
        return "{\"javaVersion\":" + Json.quote(System.getProperty("java.version"))
            + ",\"vmName\":" + Json.quote(System.getProperty("java.vm.name"))
            + ",\"vmVendor\":" + Json.quote(System.getProperty("java.vm.vendor"))
            + ",\"processors\":" + runtime.availableProcessors()
            + ",\"maxMemory\":" + runtime.maxMemory() + "}";
    }

    private String classloaders() {
        ClassLoader application = ArithmeticSample.class.getClassLoader();
        ClassLoader parent = application == null ? null : application.getParent();
        return "{\"bootstrapClass\":\"java.lang.String\",\"bootstrapLoader\":\"Bootstrap (API value: null)\""
            + ",\"sampleClass\":" + Json.quote(ArithmeticSample.class.getName())
            + ",\"sampleLoader\":" + Json.quote(loaderName(application))
            + ",\"parentLoader\":" + Json.quote(loaderName(parent)) + "}";
    }

    private String memory() {
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        MemoryUsage nonHeap = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        StringBuilder pools = new StringBuilder("[");
        List<MemoryPoolMXBean> values = ManagementFactory.getMemoryPoolMXBeans();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) pools.append(',');
            MemoryUsage usage = values.get(i).getUsage();
            pools.append("{\"name\":").append(Json.quote(values.get(i).getName()))
                .append(",\"type\":").append(Json.quote(values.get(i).getType().toString()))
                .append(",\"used\":").append(usage == null ? -1 : usage.getUsed()).append('}');
        }
        return "{\"heapUsed\":" + heap.getUsed() + ",\"heapCommitted\":" + heap.getCommitted()
            + ",\"heapMax\":" + heap.getMax() + ",\"nonHeapUsed\":" + nonHeap.getUsed()
            + ",\"pools\":" + pools.append(']').toString() + "}";
    }

    private String allocation() {
        long before = threadAllocatedBytes();
        byte[][] objects = new byte[256][];
        long payloadBytes = 0;
        for (int i = 0; i < objects.length; i++) { objects[i] = new byte[1024]; payloadBytes += objects[i].length; }
        allocationSink = objects;
        long after = threadAllocatedBytes();
        allocationSink = null;
        long allocated = before >= 0 && after >= before ? after - before : -1;
        return "{\"objectCount\":256,\"payloadBytes\":" + payloadBytes + ",\"allocatedBytes\":" + allocated
            + ",\"threadAllocationSupported\":" + (allocated >= 0) + "}";
    }

    private String references() throws InterruptedException {
        ReferenceQueue<Object> queue = new ReferenceQueue<Object>();
        Object strong = new Object();
        WeakReference<Object> weak = new WeakReference<Object>(strong, queue);
        strong = null;
        boolean collected = false;
        for (int i = 0; i < 4 && !collected; i++) {
            System.gc();
            Thread.sleep(20L);
            collected = weak.get() == null || queue.poll() != null;
        }
        return "{\"weakReferenceCleared\":" + (weak.get() == null) + ",\"collected\":" + collected
            + ",\"note\":\"System.gc is only a request; immediate collection is not guaranteed\"}";
    }

    private String garbageCollectors() {
        StringBuilder collectors = new StringBuilder("[");
        List<GarbageCollectorMXBean> values = ManagementFactory.getGarbageCollectorMXBeans();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) collectors.append(',');
            GarbageCollectorMXBean collector = values.get(i);
            collectors.append("{\"name\":").append(Json.quote(collector.getName()))
                .append(",\"count\":").append(collector.getCollectionCount())
                .append(",\"timeMs\":").append(collector.getCollectionTime()).append('}');
        }
        return "{\"collectors\":" + collectors.append(']').toString()
            + ",\"uptimeMs\":" + ManagementFactory.getRuntimeMXBean().getUptime() + "}";
    }

    private String bytecode() throws Exception {
        File executable = javapExecutable();
        Process process = new ProcessBuilder(executable.getAbsolutePath(), "-c", "-p", "-classpath",
            System.getProperty("java.class.path"), ArithmeticSample.class.getName()).redirectErrorStream(true).start();
        String output = read(process.getInputStream(), 65536);
        if (!process.waitFor(5, TimeUnit.SECONDS)) { process.destroyForcibly(); throw new IOException("javap timed out"); }
        if (process.exitValue() != 0) throw new IOException("javap failed: " + output);
        return "{\"tool\":\"javap -c -p\",\"className\":" + Json.quote(ArithmeticSample.class.getName())
            + ",\"output\":" + Json.quote(output) + "}";
    }

    private long threadAllocatedBytes() {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        if (!(bean instanceof com.sun.management.ThreadMXBean)) return -1;
        com.sun.management.ThreadMXBean sun = (com.sun.management.ThreadMXBean) bean;
        if (!sun.isThreadAllocatedMemorySupported()) return -1;
        if (!sun.isThreadAllocatedMemoryEnabled()) sun.setThreadAllocatedMemoryEnabled(true);
        return sun.getThreadAllocatedBytes(Thread.currentThread().getId());
    }

    private File javapExecutable() throws IOException {
        String suffix = System.getProperty("os.name").toLowerCase().contains("win") ? ".exe" : "";
        File home = new File(System.getProperty("java.home"));
        File direct = new File(new File(home, "bin"), "javap" + suffix);
        if (direct.isFile()) return direct;
        File parent = new File(new File(home.getParentFile(), "bin"), "javap" + suffix);
        if (parent.isFile()) return parent;
        throw new IOException("javap was not found next to the running JDK");
    }

    private String loaderName(ClassLoader loader) { return loader == null ? "Bootstrap" : loader.getClass().getName(); }
    private String response(String probe, String data) { return "{\"ok\":true,\"probe\":" + Json.quote(probe) + ",\"data\":" + data + "}"; }

    private String read(InputStream input, int maximum) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[2048];
        int count;
        while ((count = input.read(buffer)) >= 0) {
            if (output.size() + count > maximum) throw new IOException("Probe output exceeded limit");
            output.write(buffer, 0, count);
        }
        return new String(output.toByteArray(), "UTF-8");
    }
}
