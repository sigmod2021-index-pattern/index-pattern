package sigmod2021.esp.api.bridge;

/**
 * Factory interface for {@link EPBridge EPBridges}.
 * Used to construct new bridge instances without supplying parameters.
 * Typically a factory loads bridge-specific configuration and creates
 * a fully configured instance of the bridge.
 */
public interface EPBridgeFactory {

    /**
     * @return a new instance of the bridge
     */
    EPBridge createBridge();

}
