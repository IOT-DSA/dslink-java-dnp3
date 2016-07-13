package dnp3;

import java.util.HashSet;
import java.util.Set;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.serializer.Deserializer;
import org.dsa.iot.dslink.serializer.Serializer;
import org.dsa.iot.dslink.util.handler.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.serotonin.io.serial.CommPortConfigException;
import com.serotonin.io.serial.CommPortProxy;
import com.serotonin.io.serial.SerialUtils;

public class DnpLink {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DnpLink.class);
	
	private Node node;
	Serializer copySerializer;
	Deserializer copyDeserializer;
	Set<DnpOutstation> serialOutstations = new HashSet<DnpOutstation>();
	
	private DnpLink(Node node, Serializer copyser, Deserializer copydeser) {
		this.node = node;
		this.copySerializer = copyser;
		this.copyDeserializer = copydeser;
	}
	
	public static void start(Node node, Serializer copyser, Deserializer copydeser) {
		DnpLink dl = new DnpLink(node, copyser, copydeser);
		dl.init();
	}

	private void init() {
		restoreLastSession();
		
		makeAddOutstationAction(false);
		makeAddOutstationAction(true);
		makePortScanAction();
	}
	
	private void restoreLastSession() {
		if (node.getChildren() == null) return;
		for (Node child: node.getChildren().values()) {
			Value isServ = child.getAttribute("Is Serial");
			boolean isSer;
			if (isServ == null) {
				child.setAttribute("Is Serial", new Value(false));
				isSer = false;
			} else {
				isSer = isServ.getBool();
			}
			if (isSer) {
				checkAttribute(child, "COM Port", new Value("COM3"));
				checkAttribute(child, "Baud Rate", new Value(9600));
				checkAttribute(child, "Data Bits", new Value(8));
				checkAttribute(child, "Stop Bits", new Value(1));
				checkAttribute(child, "Parity", new Value(0));

			} else {
				checkAttribute(child, "Host", new Value("0.0.0.0"));
				checkAttribute(child, "Port", new Value(20000));
			}
			checkAttribute(child, "Master Address", new Value(0));
			checkAttribute(child, "Outstation Address", new Value(0));
			checkAttribute(child, "Event Polling Interval", new Value(5000));
			checkAttribute(child, "Static Polling Interval", new Value(25000));

			DnpOutstation os = new DnpOutstation(this, child);
			os.restoreLastSession();
		}
	}
	
	private static void checkAttribute(Node n, String attributeName, Value defaultValue) {
		Value val = n.getAttribute(attributeName);
		if (val == null) n.setAttribute(attributeName,  defaultValue);
	}
	
	private void makePortScanAction() {
		Action act = new Action(Permission.READ, new Handler<ActionResult>() {
			public void handle(ActionResult event) {
				doPortScan();
			}
		});
		node.createChild("scan for serial ports").setAction(act).build().setSerializable(false);
	}
	
	private void doPortScan() {
		makeAddOutstationAction(true);
		
		for (DnpOutstation serDo: serialOutstations) {
			serDo.makeEditAction();
		}
	}
	
	public Set<String> getCOMPorts() {
		Set<String> ports = new HashSet<String>();
		try {
			for (CommPortProxy p: SerialUtils.getCommPorts()) {
				ports.add(p.getId());
			}
		} catch (CommPortConfigException e) {
			LOGGER.debug("" ,e);
		}
		return ports;
	}
	
	private void makeAddOutstationAction(boolean serial) {
		if (serial) {
			Action act = new Action(Permission.READ, new Handler<ActionResult>() {
				public void handle(ActionResult event) {
					addOutstation(event);
				}
			});
			act.addParameter(new Parameter("Name", ValueType.STRING));
			
			Set<String> portids = getCOMPorts();
			if (portids.size() > 0) {
				act.addParameter(new Parameter("COM Port", ValueType.makeEnum(portids)));
				act.addParameter(new Parameter("COM Port (manual entry)", ValueType.STRING));
			} else {
				act.addParameter(new Parameter("COM Port", ValueType.STRING));
			}
			
			act.addParameter(new Parameter("Baud Rate", ValueType.NUMBER, new Value(9600)));
			act.addParameter(new Parameter("Data Bits", ValueType.NUMBER, new Value(8)));
			act.addParameter(new Parameter("Stop Bits", ValueType.NUMBER, new Value(1)));
			act.addParameter(new Parameter("Parity", ValueType.NUMBER, new Value(0)));
			act.addParameter(new Parameter("Master Address", ValueType.NUMBER, new Value(17)));
			act.addParameter(new Parameter("Outstation Address", ValueType.NUMBER, new Value(4)));
			act.addParameter(new Parameter("Event Polling Interval", ValueType.NUMBER, new Value(5)));
			act.addParameter(new Parameter("Static Polling Interval", ValueType.NUMBER, new Value(25)));
			
			Node anode = node.getChild("add serial outstation");
			if (anode == null) node.createChild("add serial outstation").setAction(act).build().setSerializable(false);
			else anode.setAction(act);
		} else {
			Action act = new Action(Permission.READ, new Handler<ActionResult>() {
				public void handle(ActionResult event) {
					addOutstation(event);
				}
			});
			act.addParameter(new Parameter("Name", ValueType.STRING));
			act.addParameter(new Parameter("Host", ValueType.STRING, new Value("0.0.0.0")));
			act.addParameter(new Parameter("Port", ValueType.NUMBER, new Value(20000)));
			act.addParameter(new Parameter("Master Address", ValueType.NUMBER, new Value(17)));
			act.addParameter(new Parameter("Outstation Address", ValueType.NUMBER, new Value(4)));
			act.addParameter(new Parameter("Event Polling Interval", ValueType.NUMBER, new Value(5)));
			act.addParameter(new Parameter("Static Polling Interval", ValueType.NUMBER, new Value(25)));
			
			Node anode = node.getChild("add ip outstation");
			if (anode == null) node.createChild("add ip outstation").setAction(act).build().setSerializable(false);
			else anode.setAction(act);
		}
		
	}
	
	private void addOutstation(ActionResult event) {
		String name = event.getParameter("Name", ValueType.STRING).getString();
		boolean isSer = (event.getParameter("Host") == null);
		
		Node onode = node.createChild(name).build();
		onode.setAttribute("Is Serial", new Value(isSer));
		
		if (isSer) {
			String com;
			Value customPort = event.getParameter("COM Port (manual entry)");
			if (customPort != null && customPort.getString() != null && customPort.getString().trim().length() > 0) {
				com = customPort.getString();
			} else {
				com = event.getParameter("COM Port").getString();
			}
			int baud = event.getParameter("Baud Rate", ValueType.NUMBER).getNumber().intValue();
			int dbits = event.getParameter("Data Bits", ValueType.NUMBER).getNumber().intValue();
			int sbits = event.getParameter("Stop Bits", ValueType.NUMBER).getNumber().intValue();
			int parity = event.getParameter("Parity", ValueType.NUMBER).getNumber().intValue();
		
			onode.setAttribute("COM Port", new Value(com));
			onode.setAttribute("Baud Rate", new Value(baud));
			onode.setAttribute("Data Bits", new Value(dbits));
			onode.setAttribute("Stop Bits", new Value(sbits));
			onode.setAttribute("Parity", new Value(parity));
		} else {
			String host = event.getParameter("Host", ValueType.STRING).getString();
			int port = event.getParameter("Port", ValueType.NUMBER).getNumber().intValue();
			
			onode.setAttribute("Host", new Value(host));
			onode.setAttribute("Port", new Value(port));
		}
		
		int maddr = event.getParameter("Master Address", ValueType.NUMBER).getNumber().intValue();
		int oaddr = event.getParameter("Outstation Address", ValueType.NUMBER).getNumber().intValue();
		long interval = (long) (event.getParameter("Event Polling Interval", ValueType.NUMBER).getNumber().doubleValue() * 1000);
		long sinterval = (long) (event.getParameter("Static Polling Interval", ValueType.NUMBER).getNumber().doubleValue() * 1000);
		
		onode.setAttribute("Master Address", new Value(maddr));
		onode.setAttribute("Outstation Address", new Value(oaddr));
		onode.setAttribute("Event Polling Interval", new Value(interval));
		onode.setAttribute("Static Polling Interval", new Value(sinterval));
		DnpOutstation os = new DnpOutstation(this, onode);
		os.init();
	}

}
