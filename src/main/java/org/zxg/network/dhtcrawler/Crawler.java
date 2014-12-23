/*
 * Copyright (C) 2014 Xianguang Zhou <xianguang.zhou@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.zxg.network.dhtcrawler;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.codec.binary.Hex;
import org.zxg.network.dhtcrawler.bt.BtCrawler;
import org.zxg.network.dhtcrawler.dht.CompactIpAddressPortInfo;
import org.zxg.network.dhtcrawler.dht.CompactNodeInfo;
import org.zxg.network.dhtcrawler.dht.Dht;
import org.zxg.network.dhtcrawler.dht.DhtAnnouncePeerReply;
import org.zxg.network.dhtcrawler.dht.DhtAnnouncePeerReq;
import org.zxg.network.dhtcrawler.dht.DhtException;
import org.zxg.network.dhtcrawler.dht.DhtFindNodeReply;
import org.zxg.network.dhtcrawler.dht.DhtFindNodeReq;
import org.zxg.network.dhtcrawler.dht.DhtGetPeersReply;
import org.zxg.network.dhtcrawler.dht.DhtGetPeersReq;
import org.zxg.network.dhtcrawler.dht.DhtMsg;
import org.zxg.network.dhtcrawler.dht.DhtPingReply;
import org.zxg.network.dhtcrawler.dht.DhtPingReq;
import org.zxg.network.dhtcrawler.dht.DhtReply;
import org.zxg.network.dhtcrawler.dht.DhtReq;
import org.zxg.network.dhtcrawler.krpc.KrpcException;

/**
 *
 * @author Xianguang Zhou <xianguang.zhou@outlook.com>
 */
public class Crawler extends Dht {

    private final static List<Addr> BOOTSTRAP_NODES = new LinkedList<>();

    static {
        BOOTSTRAP_NODES.add(new Addr("router.bittorrent.com", 6881));
        BOOTSTRAP_NODES.add(new Addr("dht.transmissionbt.com", 6881));
        BOOTSTRAP_NODES.add(new Addr("router.utorrent.com", 6881));
    }

    private String host;
    private int port;

    private byte[] nodeId;
    private RouteTable routeTable;
    private Map<String, MethodAndTime> dhtReqMethodCache;

    private DatagramSocket socket;
    private ReceiveThread receiveThread;
    private Timer timer;

    private CrawlerListener crawlerListener;

    private BtCrawler btCrawler;

    public Crawler(String host, int port, CrawlerListener crawlerListener) {
        this.host = host;
        this.port = port;
        this.crawlerListener = crawlerListener;
    }

    public void start() throws SocketException, NoSuchAlgorithmException {
        btCrawler = new BtCrawler(this.crawlerListener);
        btCrawler.start();
        nodeId = Util.randomId();
        routeTable = new RouteTable(nodeId);
        dhtReqMethodCache = new ConcurrentHashMap<>();
        socket = new DatagramSocket(new InetSocketAddress(host, port));
        receiveThread = new ReceiveThread();
        receiveThread.start();
        timer = new Timer(true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                clearDhtReqMethodCache();
            }
        }, 900000, 900000);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    maintainRouteTable();
                } catch (Exception ex) {
                    Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }, 1000, 1000);
    }

    public void stop() {
        timer.cancel();
        try {
            receiveThread.cancel();
            receiveThread.join();
        } catch (InterruptedException ex) {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        }
        socket.close();
        btCrawler.stop();
    }

    private void clearDhtReqMethodCache() {
        long currentTime = System.currentTimeMillis();
        this.dhtReqMethodCache
                .entrySet().stream().filter((entry) -> (currentTime - entry.getValue().time > 900000)).forEach((entry) -> {
                    this.dhtReqMethodCache.remove(entry.getKey());
                });
    }

    private void maintainRouteTable() throws IOException {
        long currentTime = System.currentTimeMillis();
        for (Bucket bucket : routeTable.buckets) {
            if (currentTime - bucket.lastChangeTime > 900000) {
                byte[] randomId = Util.randomId(bucket.min, bucket.max);
                findNodes(randomId);
            }
            for (Node node : bucket.nodes) {
                if (!((currentTime - node.lastReplyTime < 900000) || (node.replied && currentTime - node.lastReqTime < 900000))) {
                    pingNode(new Addr(node.ip, node.port), this.nodeId);
                    bucket.nodes.remove(node);
                }
            }
        }
        findNodes(this.nodeId);
    }

    private void receive() throws IOException, DhtException, KrpcException {
        DhtMsg dhtMsg = recvDht();
        Node addedNode = addRouteTableNode(dhtMsg);
        if (dhtMsg instanceof DhtGetPeersReq) {
            DhtGetPeersReq dhtGetPeersReq = (DhtGetPeersReq) dhtMsg;
            receive(dhtGetPeersReq, addedNode);
        } else if (dhtMsg instanceof DhtGetPeersReply) {
            DhtGetPeersReply dhtGetPeersReply = (DhtGetPeersReply) dhtMsg;
            receive(dhtGetPeersReply);
        } else if (dhtMsg instanceof DhtFindNodeReq) {
            DhtFindNodeReq dhtFindNodeReq = (DhtFindNodeReq) dhtMsg;
            receive(dhtFindNodeReq);
        } else if (dhtMsg instanceof DhtFindNodeReply) {
            DhtFindNodeReply dhtFindNodeReply = (DhtFindNodeReply) dhtMsg;
            receive(dhtFindNodeReply);
        } else if (dhtMsg instanceof DhtPingReq) {
            DhtPingReq dhtPingReq = (DhtPingReq) dhtMsg;
            receive(dhtPingReq);
        } else if (dhtMsg instanceof DhtPingReply) {
            DhtPingReply dhtPingReply = (DhtPingReply) dhtMsg;
            receive(dhtPingReply);
        } else if (dhtMsg instanceof DhtAnnouncePeerReq) {
            DhtAnnouncePeerReq dhtAnnouncePeerReq = (DhtAnnouncePeerReq) dhtMsg;
            receive(dhtAnnouncePeerReq, addedNode);
        } else if (dhtMsg instanceof DhtAnnouncePeerReply) {
            DhtAnnouncePeerReply dhtAnnouncePeerReply = (DhtAnnouncePeerReply) dhtMsg;
            receive(dhtAnnouncePeerReply);
        }
    }

    private byte[] generateGetPeersToken() {
        return Util.entropy(4);
    }

    private void receive(DhtGetPeersReq dhtGetPeersReq, Node addedNode) throws IOException {
        DhtGetPeersReply dhtGetPeersReply = new DhtGetPeersReply();
        dhtGetPeersReply.addr = dhtGetPeersReq.addr;
        dhtGetPeersReply.tId = dhtGetPeersReq.tId;
        dhtGetPeersReply.nodeId = this.nodeId;
        if (addedNode != null) {
            if (addedNode.getPeersTokens == null) {
                addedNode.getPeersTokens = new LinkedList<>();
            }
            if (addedNode.getPeersTokens.isEmpty() || System.currentTimeMillis() - addedNode.getPeersTokens.get(0).time > 300000) {
                GetPeersTokenAndTime getPeersTokenAndTime = new GetPeersTokenAndTime();
                getPeersTokenAndTime.token = generateGetPeersToken();
                getPeersTokenAndTime.time = System.currentTimeMillis();
                addedNode.getPeersTokens.add(0, getPeersTokenAndTime);
                dhtGetPeersReply.token = getPeersTokenAndTime.token;
            } else {
                dhtGetPeersReply.token = addedNode.getPeersTokens.get(0).token;
            }
        } else {
            dhtGetPeersReply.token = generateGetPeersToken();
        }
        dhtGetPeersReply.nodes = new LinkedList<>();
        routeTable.nearestNodes(dhtGetPeersReq.infoHash).stream().map(node -> {
            CompactNodeInfo compactNodeInfo = new CompactNodeInfo();
            compactNodeInfo.nodeId = node.id;
            compactNodeInfo.compactIpAddressPortInfo = new CompactIpAddressPortInfo();
            compactNodeInfo.compactIpAddressPortInfo.ip = node.ip;
            compactNodeInfo.compactIpAddressPortInfo.port = node.port;
            return compactNodeInfo;
        }).forEach((compactNodeInfo) -> {
            dhtGetPeersReply.nodes.add(compactNodeInfo);
        });
        sendDht(dhtGetPeersReply);
        if (addedNode != null) {
            long currentTime = System.currentTimeMillis();
            while (!addedNode.getPeersTokens.isEmpty()) {
                int lastIndex = addedNode.getPeersTokens.size() - 1;
                GetPeersTokenAndTime lastGetPeersTokenAndTime = addedNode.getPeersTokens.get(lastIndex);
                if (currentTime - lastGetPeersTokenAndTime.time > 600000) {
                    addedNode.getPeersTokens.remove(lastIndex);
                } else {
                    break;
                }
            }
        }
        crawlerListener.getPeers(dhtGetPeersReq.infoHash);
    }

    private void receive(DhtGetPeersReply dhtGetPeersReply) {
    }

    private void receive(DhtFindNodeReq dhtFindNodeReq) throws IOException {
        DhtFindNodeReply dhtFindNodeReply = new DhtFindNodeReply();
        dhtFindNodeReply.addr = dhtFindNodeReq.addr;
        dhtFindNodeReply.tId = dhtFindNodeReq.tId;
        dhtFindNodeReply.nodeId = this.nodeId;
        dhtFindNodeReply.nodes = new LinkedList<>();
        routeTable.nearestNodes(dhtFindNodeReq.targetNodeId).stream().map((node) -> {
            CompactNodeInfo compactNodeInfo = new CompactNodeInfo();
            compactNodeInfo.nodeId = node.id;
            compactNodeInfo.compactIpAddressPortInfo = new CompactIpAddressPortInfo();
            compactNodeInfo.compactIpAddressPortInfo.ip = node.ip;
            compactNodeInfo.compactIpAddressPortInfo.port = node.port;
            return compactNodeInfo;
        }).forEach((compactNodeInfo) -> {
            dhtFindNodeReply.nodes.add(compactNodeInfo);
        });
        sendDht(dhtFindNodeReply);
    }

    private void receive(DhtFindNodeReply dhtFindNodeReply) throws IOException {
        for (CompactNodeInfo compactNodeInfo : dhtFindNodeReply.nodes) {
            try {
                this.routeTable.add(new Node(compactNodeInfo.nodeId, compactNodeInfo.compactIpAddressPortInfo.ip, compactNodeInfo.compactIpAddressPortInfo.port));
            } catch (BucketFullException ex) {
                Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
            }
            findNode(new Addr(compactNodeInfo.compactIpAddressPortInfo.ip, compactNodeInfo.compactIpAddressPortInfo.port), this.nodeId);
        }
    }

    private void receive(DhtPingReq dhtPingReq) throws IOException {
        DhtPingReply dhtPingReply = new DhtPingReply();
        dhtPingReply.addr = dhtPingReq.addr;
        dhtPingReply.tId = dhtPingReq.tId;
        dhtPingReply.nodeId = this.nodeId;
        sendDht(dhtPingReply);
    }

    private void receive(DhtPingReply dhtPingReply) {
    }

    private void receive(DhtAnnouncePeerReq dhtAnnouncePeerReq, Node addedNode) throws IOException {
        DhtAnnouncePeerReply dhtAnnouncePeerReply = new DhtAnnouncePeerReply();
        dhtAnnouncePeerReply.addr = dhtAnnouncePeerReq.addr;
        dhtAnnouncePeerReply.tId = dhtAnnouncePeerReq.tId;
        dhtAnnouncePeerReply.nodeId = this.nodeId;
        sendDht(dhtAnnouncePeerReply);
//        System.out.println("announce peer:" + Hex.encodeHexString(dhtAnnouncePeerReq.infoHash) + " " + Hex.encodeHexString(dhtAnnouncePeerReq.token)); // TODO
//        if (addedNode != null && addedNode.getPeersTokens != null) {
//            long currentTime = System.currentTimeMillis();
//            for (GetPeersTokenAndTime getPeersTokenAndTime : addedNode.getPeersTokens) {
//                System.out.println("cached token:" + Hex.encodeHexString(getPeersTokenAndTime.token));
//                if (currentTime - getPeersTokenAndTime.time > 600000) {
//                    break;
//                }
//                if (Arrays.equals(getPeersTokenAndTime.token, dhtAnnouncePeerReq.token)) {
        if (crawlerListener.announcePeer(dhtAnnouncePeerReq.infoHash)) {
            btCrawler.downloadMetaData(dhtAnnouncePeerReq.infoHash, dhtAnnouncePeerReq.addr.ip, dhtAnnouncePeerReq.port, dhtAnnouncePeerReq.nodeId);
        }
//                    break;
//                }
//            }
//        }
    }

    private void receive(DhtAnnouncePeerReply dhtAnnouncePeerReply) {
    }

    private Node addRouteTableNode(DhtMsg dhtMsg) {
        final Ref<Node> addedNodeRef = new Ref<>();
        try {
            if (dhtMsg instanceof DhtReq) {
                DhtReq dhtReq = (DhtReq) dhtMsg;
                addedNodeRef.target = new Node(dhtReq.nodeId, dhtReq.addr.ip, dhtReq.addr.port);
                routeTable.add(addedNodeRef.target, (newNode, oldNode) -> {
                    oldNode.ip = newNode.ip;
                    oldNode.port = newNode.port;
                    oldNode.lastReqTime = newNode.lastReqTime;
                    addedNodeRef.target = oldNode;
                });
            } else if (dhtMsg instanceof DhtReply) {
                DhtReply dhtReply = (DhtReply) dhtMsg;
                addedNodeRef.target = new Node(dhtReply.nodeId, dhtReply.addr.ip, dhtReply.addr.port);
                routeTable.add(addedNodeRef.target, (newNode, oldNode) -> {
                    oldNode.ip = newNode.ip;
                    oldNode.port = newNode.port;
                    oldNode.lastReplyTime = newNode.lastReplyTime;
                    oldNode.replied = true;
                    addedNodeRef.target = oldNode;
                });
            }
        } catch (BucketFullException ex) {
            addedNodeRef.target = null;
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        }
        return addedNodeRef.target;
    }

    private void pingNode(Addr addr, byte[] nodeId) throws IOException {
        DhtPingReq req = new DhtPingReq();
        req.addr = addr;
        req.nodeId = nodeId;
        sendDht(req);
    }

    private void findNodes(byte[] targetNodeId) throws IOException {
        List<Node> nearestNodes = routeTable.nearestNodes(targetNodeId);
        if (nearestNodes.isEmpty()) {
            for (Addr addr : BOOTSTRAP_NODES) {
                findNode(addr, targetNodeId);
            }
        } else {
            for (Node node : nearestNodes) {
                findNode(new Addr(node.ip, node.port), targetNodeId);
            }
        }
    }

    private void findNode(Addr addr, byte[] targetNodeId) throws IOException {
        DhtFindNodeReq req = new DhtFindNodeReq();
        req.addr = addr;
        req.nodeId = this.nodeId;
        req.targetNodeId = targetNodeId;
        sendDht(req);
    }

    private class ReceiveThread extends Thread {

        private boolean running;

        public ReceiveThread() {
            super();
            setDaemon(true);
            running = true;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    receive();
                } catch (DhtException | KrpcException | IOException ex) {
                    Logger.getLogger(ReceiveThread.class.getName()).log(Level.FINE, null, ex);
                } catch (Exception ex) {
                    Logger.getLogger(ReceiveThread.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

        public void cancel() {
            running = false;
        }
    }

    public void join() throws InterruptedException {
        receiveThread.join();
    }

    public static void main(String[] args) throws Exception {
        Crawler clawer = new Crawler("0.0.0.0", 6881, new CrawlerListener() {
            @Override
            public void getPeers(byte[] infoHash) {
                System.out.println("infohash:" + Hex.encodeHexString(infoHash));
            }

            @Override
            public void metaData(byte[] infoHash, MetaData metaData) {
                System.out.println("name:" + metaData.name);
            }
        });
        clawer.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> clawer.stop()));
        clawer.join();
    }

    @Override
    protected void sendUdp(UdpMsg msg) throws IOException {
        DatagramPacket datagramPacket = new DatagramPacket(msg.data,
                msg.data.length);
        datagramPacket.setSocketAddress(new InetSocketAddress(msg.addr.ip,
                msg.addr.port));
        socket.send(datagramPacket);
    }

    @Override
    protected UdpMsg recvUdp() throws IOException {
        byte[] buffer = new byte[65536];
        DatagramPacket datagramPacket = new DatagramPacket(buffer,
                buffer.length);
        socket.receive(datagramPacket);
        UdpMsg msg = new UdpMsg();
        msg.data = new byte[datagramPacket.getLength()];
        System.arraycopy(datagramPacket.getData(), datagramPacket.getOffset(),
                msg.data, 0, datagramPacket.getLength());
        InetSocketAddress socketAddress = (InetSocketAddress) datagramPacket
                .getSocketAddress();
        msg.addr = new Addr(socketAddress.getHostString(), socketAddress.getPort());
        return msg;
    }

    @Override
    protected void setDhtReqMethod(Addr addr, byte[] tid, String method) {
//        dhtReqMethodCache.put(
//                addr.ip + ":" + addr.port + "/"
//                + Hex.encodeHexString(tid), new MethodAndTime(method,
//                        System.currentTimeMillis()));
        dhtReqMethodCache.put(Hex.encodeHexString(tid), new MethodAndTime(method,
                System.currentTimeMillis()));
    }

    @Override
    protected String removeDhtReqMethod(Addr addr, byte[] tid) {
//        MethodAndTime methodAndTime = dhtReqMethodCache.remove(addr.ip + ":" + addr.port
//                + "/" + Hex.encodeHexString(tid));
        MethodAndTime methodAndTime = dhtReqMethodCache.remove(Hex.encodeHexString(tid));
        if (methodAndTime != null) {
            return methodAndTime.method;
        } else {
            return "unknown";
        }
    }
}
