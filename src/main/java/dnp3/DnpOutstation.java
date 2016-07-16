package dnp3;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.Writable;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValuePair;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.dslink.util.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.org.scadabr.dnp34j.master.common.AppFeatures;
import br.org.scadabr.dnp34j.master.common.utils.Buffer;
import br.org.scadabr.dnp34j.master.session.DNPUser;
import br.org.scadabr.dnp34j.master.session.config.DNPConfig;
import br.org.scadabr.dnp34j.master.session.config.EthernetParameters;
import br.org.scadabr.dnp34j.master.session.config.SerialParameters;
import br.org.scadabr.dnp34j.master.session.database.DataBuffer;
import br.org.scadabr.dnp34j.master.session.database.DataElement;
import br.org.scadabr.dnp34j.master.session.database.Database;

public class DnpOutstation {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DnpOutstation.class);
	
	private DnpLink link;
	private Node node;
	private DNPUser user;
	private boolean isSerial;
	
	private Node ainode;
	private Node binode;
	private Node dinode;
	private Node cinode;
	private Node aonode;
	private Node bonode;
	
	
	private int pollsPerDiscover = 1;
	private int pollsSinceLastDiscover = 0;
	
	private Set<Node> subscribed = ConcurrentHashMap.newKeySet();
	ScheduledFuture<?> future;
	
	DnpOutstation(DnpLink link, Node node) {
		this.link = link;
		this.node = node;
		isSerial = node.getAttribute("Is Serial").getBool();
		if (isSerial) link.serialOutstations.add(this);
	}
	
	void init() {
		
		int maddr = node.getAttribute("Master Address").getNumber().intValue();
		int oaddr = node.getAttribute("Outstation Address").getNumber().intValue();
		
		DNPConfig configuration;
		if (isSerial) {
			String com = node.getAttribute("COM Port").getString();
			int baud = node.getAttribute("Baud Rate").getNumber().intValue();
			int dbits = node.getAttribute("Data Bits").getNumber().intValue();
			int sbits = node.getAttribute("Stop Bits").getNumber().intValue();
			int parity = node.getAttribute("Parity").getNumber().intValue();
			
			SerialParameters params = new SerialParameters(com, baud, dbits, sbits, parity);
			configuration = new DNPConfig(params, maddr, oaddr);
		} else {
			String host = node.getAttribute("Host").getString();
			int port = node.getAttribute("Port").getNumber().intValue();
			
			EthernetParameters params = new EthernetParameters(host, port);
	        configuration = new DNPConfig(params, maddr, oaddr);
		}
		
        user = new DNPUser(configuration);
        try {
			user.init();
		} catch (Exception e) {
			LOGGER.debug("", e);
		}
        
        if (user.getDatabase() != null) {
	        user.getDatabase().setCallback(new Database.Handler() {
				public void dataChanged(DataElement element) {
					updateValue(element);
				}
			});
        }
        
        long interval = node.getAttribute("Event Polling Interval").getNumber().longValue();
        long sinterval = node.getAttribute("Static Polling Interval").getNumber().longValue();
        if (sinterval < interval) {
        	node.setAttribute("Event Polling Interval", new Value(sinterval));
        	pollsPerDiscover = 1;
        } else {
        	pollsPerDiscover = (int) (sinterval/interval);
        }
        
        makeRemoveAction();
        makeEditAction();
        makeDiscoverAction();
	}
	
	void restoreLastSession() {
		node.clearChildren();
		init();
	}
	
	private void makeRemoveAction() {
		Action act = new Action(Permission.READ, new Handler<ActionResult>() { 
			public void handle(ActionResult event) {
				remove();
			}
		});
		Node anode = node.getChild("remove");
		if (anode == null) node.createChild("remove").setAction(act).build().setSerializable(false);
		else anode.setAction(act);
	}
	
	private void remove() {
		stop();
		node.clearChildren();
		node.getParent().removeChild(node);
		link.serialOutstations.remove(this);
	}
	
	private void stop() {
		if (user != null) {
			try {
				user.stop();
			} catch (Exception e) {
				LOGGER.debug("", e);
			} 
		}
	}
	
	void makeEditAction() {
		Action act = new Action(Permission.READ, new Handler<ActionResult>() {
			public void handle(ActionResult event) {
				edit(event);
			}
		});

		act.addParameter(new Parameter("Name", ValueType.STRING, new Value(node.getName())));
		if (isSerial) {
			Set<String> portids = link.getCOMPorts();
			if (portids.size() > 0) {
				if (portids.contains(node.getAttribute("COM Port").getString())) {
					act.addParameter(new Parameter("COM Port", ValueType.makeEnum(portids), node.getAttribute("COM Port")));
					act.addParameter(new Parameter("COM Port (manual entry)", ValueType.STRING));
				} else {
					act.addParameter(new Parameter("COM Port", ValueType.makeEnum(portids)));
					act.addParameter(new Parameter("COM Port (manual entry)", ValueType.STRING, node.getAttribute("COM Port")));
				}
			} else {
				act.addParameter(new Parameter("COM Port", ValueType.STRING, node.getAttribute("COM Port")));
			}
			act.addParameter(new Parameter("Baud Rate", ValueType.NUMBER, node.getAttribute("Baud Rate")));
			act.addParameter(new Parameter("Data Bits", ValueType.NUMBER, node.getAttribute("Data Bits")));
			act.addParameter(new Parameter("Stop Bits", ValueType.NUMBER, node.getAttribute("Stop Bits")));
			act.addParameter(new Parameter("Parity", ValueType.NUMBER, node.getAttribute("Parity")));
		} else {
			act.addParameter(new Parameter("Host", ValueType.STRING, node.getAttribute("Host")));
			act.addParameter(new Parameter("Port", ValueType.NUMBER, node.getAttribute("Port")));
		}
		act.addParameter(new Parameter("Master Address", ValueType.NUMBER, node.getAttribute("Master Address")));
		act.addParameter(new Parameter("Outstation Address", ValueType.NUMBER, node.getAttribute("Outstation Address")));
		double defint = node.getAttribute("Event Polling Interval").getNumber().doubleValue() / 1000;
		act.addParameter(new Parameter("Event Polling Interval", ValueType.NUMBER, new Value(defint)));
		double defsint = node.getAttribute("Static Polling Interval").getNumber().doubleValue() / 1000;
		act.addParameter(new Parameter("Static Polling Interval", ValueType.NUMBER, new Value(defsint)));
		Node anode = node.getChild("edit");
		if (anode == null) node.createChild("edit").setAction(act).build().setSerializable(false);
		else anode.setAction(act);
	}
	
	private void edit(ActionResult event) {
		String name = event.getParameter("Name", ValueType.STRING).getString();
		
		if (isSerial) {
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
		
			node.setAttribute("COM Port", new Value(com));
			node.setAttribute("Baud Rate", new Value(baud));
			node.setAttribute("Data Bits", new Value(dbits));
			node.setAttribute("Stop Bits", new Value(sbits));
			node.setAttribute("Parity", new Value(parity));
		} else {
			String host = event.getParameter("Host", ValueType.STRING).getString();
			int port = event.getParameter("Port", ValueType.NUMBER).getNumber().intValue();
		
			node.setAttribute("Host", new Value(host));
			node.setAttribute("Port", new Value(port));
		}
		
		int maddr = event.getParameter("Master Address", ValueType.NUMBER).getNumber().intValue();
		int oaddr = event.getParameter("Outstation Address", ValueType.NUMBER).getNumber().intValue();
		long interval = (long) (event.getParameter("Event Polling Interval", ValueType.NUMBER).getNumber().doubleValue() * 1000);
		long sinterval = (long) (event.getParameter("Static Polling Interval", ValueType.NUMBER).getNumber().doubleValue() * 1000);
		
		node.setAttribute("Master Address", new Value(maddr));
		node.setAttribute("Outstation Address", new Value(oaddr));
		node.setAttribute("Event Polling Interval", new Value(interval));
		node.setAttribute("Static Polling Interval", new Value(sinterval));
		
		if (!node.getName().equals(name)) {
			rename(name);
		} else {
			stop();
			init();
		}
	}
	
	private void rename(String newname) {
		duplicate(newname);
		remove();
	}
	
	private void duplicate(String name) {
		JsonObject jobj = link.copySerializer.serialize();
		JsonObject nodeobj =  jobj.get(node.getName());
		jobj.put(name, nodeobj);
		link.copyDeserializer.deserialize(jobj);
		Node newnode = node.getParent().getChild(name);
		DnpOutstation os = new DnpOutstation(link, newnode);
		os.restoreLastSession();
	}
	
	private void makeDiscoverAction() {
		Action act = new Action(Permission.READ, new Handler<ActionResult>() {
			public void handle(ActionResult event) {
				discover();
			}
		});
		Node anode = node.getChild("discover");
		if (anode == null) node.createChild("discover").setAction(act).build().setSerializable(false);
		else anode.setAction(act);
		
		act = new Action(Permission.READ, new Handler<ActionResult>() {
			public void handle(ActionResult event) {
				update();
			}
		});
		anode = node.getChild("update");
		if (anode == null) node.createChild("update").setAction(act).build().setSerializable(false);
		else anode.setAction(act);
	}
	
	private void discover() {
		
		try {
			user.sendSynch(user.buildReadStaticDataMsg());
		} catch (Exception e) {
			LOGGER.debug("", e);
		}
		
		
	}
	
	private void update() {
		try {
			user.sendSynch(user.buildReadEventDataMsg());
		} catch (Exception e) {
			LOGGER.debug("" ,e);
		}
	}
	
	private void updateValue(DataElement element) {
		DataType dt = DataType.getGroupType(element.getGroup());
		Integer num = element.getIndex();
		String valStr = element.getValue();
		updateNode(dt, num, valStr);
		
	}
	
	private void updateValues() {
		Database db = user.getDatabase();
		handleStaticData(DataType.BI, db.getBinaryInputPoints());
		handleStaticData(DataType.DI, db.getDoubleInputPoints());
		handleStaticData(DataType.AI, db.getAnalogInputPoints());
		handleStaticData(DataType.CI, db.getCounterInputPoints());
		handleStaticData(DataType.BO, db.getBinaryOutputPoints());
		handleStaticData(DataType.AO, db.getAnalogOutputPoints());
	}
	
	private void handleStaticData(DataType type, HashMap<Integer, DataBuffer> data) {
		if (data == null || data.isEmpty()) return;
		for (Entry<Integer, DataBuffer> entry: data.entrySet()) {
			Integer num = entry.getKey();
			DataBuffer valBuf = entry.getValue();
			String valStr = valBuf.readLastRecord().getValue();
			updateNode(type, num, valStr);			
		}
	}
	
	private void updateNode(final DataType type, final int index, String value) {
		String pointName = type.toString() + " " + Integer.toString(index);
		Node dataNode = getDataNode(type);
		Node pointNode = dataNode.getChild(pointName);
		if (pointNode == null) {
			pointNode = dataNode.createChild(pointName).setValueType(type.getValueType()).setValue(type.makeValue(value)).build();
			pointNode.getListener().setOnSubscribeHandler(new Handler<Node>() {
				public void handle(Node event) {
					subscribe(event);
				}
			});
			pointNode.getListener().setOnUnsubscribeHandler(new Handler<Node>() {
				public void handle(Node event) {
					unsubscribe(event);
				}
			});
			if (type == DataType.BO || type == DataType.AO) {
				pointNode.setWritable(Writable.WRITE);
				pointNode.getListener().setValueHandler(new Handler<ValuePair>() {
					public void handle(ValuePair event) {
						if (!event.isFromExternalSource()) return;
						Value val = event.getCurrent();
		        		if (!val.equals(event.getPrevious())) handleSet(type, index, val);
					}
				});
			}
		} else {
			pointNode.setValue(type.makeValue(value));
		}
	}
	
	private void handleSet(DataType type, int index, Value val) {
		Buffer cmd = null;
		if (type == DataType.AO) {
			cmd = user.buildAnalogControlCommand(AppFeatures.DIRECT_OPERATE, index, val.getNumber().doubleValue());
		} else if (type == DataType.BO) {
			byte code = val.getBool().booleanValue() ? (byte) 1 : (byte) 0;
			cmd = user.buildBinaryControlCommand(AppFeatures.DIRECT_OPERATE, index, code, 0, 0);
		}
		if (cmd != null) {
			try {
				user.sendSynch(cmd);
			} catch (Exception e) {
				LOGGER.debug("" ,e);
			}
			discover();
		}
		
	}
	
	private static boolean databaseIsEmpty(Database db) {
		if (db == null) return true;
		return (db.getAnalogInputPoints().isEmpty() && db.getAnalogOutputPoints().isEmpty() &&
				db.getBinaryInputPoints().isEmpty() && db.getBinaryOutputPoints().isEmpty() &&
				db.getCounterInputPoints().isEmpty());
	}
	
	private enum DataType {
		BI("Binary Input", 0x01), AI("Analog Input", 0x30), CI("Counter Input", 0x20), BO("Control Output", 0x10), AO("Analog Output", 0x40), DI("Double Input", 0x03);
	
		private String name;
		private String nodeName;
		private int group;
		
		private DataType(String name, int group) {
			this.name = name;
			this.nodeName = name + "s";
			this.group = group;
		}
		
		@Override
		public String toString() {
			return name;
		}
		
		public static DataType getGroupType(int group) {
			switch(group) {
			case 0x00:
			case 0x01:
				return BI;
			case 0x03:
				return DI;
			case 0x10:
				return BO;
			case 0x20:
				return CI;
			case 0x30:
				return AI;
			case 0x40:
				return AO;
			default:
				return null;
			}
		}
		
		public ValueType getValueType() {
			switch(this) {
			case BI:
			case BO:
				return ValueType.BOOL;
			case DI:
				return ValueType.makeEnum("Intermediate", "Off", "On", "Indeterminate");
			default:
				return ValueType.NUMBER;
			}
		}
		
		public Value makeValue(String str) {
			switch(this) {
			case BI:
			case BO:
				return new Value(Boolean.parseBoolean(str));
			case DI:
				return new Value(str);
			default:
				return new Value(Double.parseDouble(str));
			}
		}
	}
	
	private Node getDataNode(DataType type) {
		switch(type) {
		case BI: {
			if (binode == null) binode = node.createChild(type.nodeName).build();
			return binode;
		}
		case DI: {
			if (dinode == null) dinode = node.createChild(type.nodeName).build();
			return dinode;
		}
		case AI: {
			if (ainode == null) ainode = node.createChild(type.nodeName).build();
			return ainode;
		}
		case CI: {
			if (cinode == null) cinode = node.createChild(type.nodeName).build();
			return cinode;
		}
		case BO: {
			if (bonode == null) bonode = node.createChild(type.nodeName).build();
			return bonode;
		}
		case AO: {
			if (aonode == null) aonode = node.createChild(type.nodeName).build();
			return aonode;
		}
		}
		return null;
	}
	
	private void subscribe(Node event) {
		boolean wasEmpty = subscribed.isEmpty();
		subscribed.add(event);
		if (wasEmpty) startPoll();
	}
	
	private void unsubscribe(Node event) {
		subscribed.remove(event);
		if (subscribed.isEmpty()) stopPoll();
	}
	
	private void startPoll() {
		if (future != null) return;
		long interval = node.getAttribute("Event Polling Interval").getNumber().longValue();
		ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
		future = stpe.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				if (pollsSinceLastDiscover < pollsPerDiscover - 1) {
					try {
						user.sendSynch(user.buildReadEventDataMsg());
					} catch (Exception e) {
						LOGGER.debug("" ,e);
					}
					pollsSinceLastDiscover += 1;
				} else {
					discover();
					pollsSinceLastDiscover = 0;
				}
			}
		}, 0, interval, TimeUnit.MILLISECONDS);
	}
	
	private void stopPoll() {
		if (future != null) future.cancel(false);
		future = null;
	}
	
	

}
