package lab.jvm;

public final class JvmProbeTest {
    public static void main(String[] args) throws Exception {
        JvmProbeService service = new JvmProbeService();
        assertContains(service.probe("runtime"), "javaVersion");
        assertContains(service.probe("classloaders"), "Bootstrap");
        assertContains(service.probe("memory"), "heapUsed");
        assertContains(service.probe("allocation"), "allocatedBytes");
        assertContains(service.probe("references"), "collected");
        assertContains(service.probe("gc"), "collectors");
        String bytecode = service.probe("bytecode");
        assertContains(bytecode, "iadd");
        assertContains(bytecode, "ireturn");
        assertThrowsUnknown(service);
        System.out.println("JvmProbeTest: PASS");
    }

    private static void assertContains(String value, String expected) {
        if (!value.contains(expected)) throw new AssertionError("Expected response to contain " + expected + ": " + value);
    }

    private static void assertThrowsUnknown(final JvmProbeService service) {
        try { service.probe("shell"); }
        catch (IllegalArgumentException expected) { return; }
        catch (Exception error) { throw new AssertionError(error); }
        throw new AssertionError("Expected unknown probe to be rejected");
    }
}
