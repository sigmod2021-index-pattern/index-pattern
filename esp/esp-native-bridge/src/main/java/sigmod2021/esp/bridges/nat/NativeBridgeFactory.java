package sigmod2021.esp.bridges.nat;

import org.kohsuke.MetaInfServices;
import sigmod2021.esp.api.bridge.EPBridge;
import sigmod2021.esp.api.bridge.EPBridgeFactory;

@MetaInfServices
public class NativeBridgeFactory implements EPBridgeFactory {

    @Override
    public EPBridge createBridge() {
        return new NativeBridge();
    }

    @Override
    public String toString() {
        return "NativeBridge Factory";
    }


}
