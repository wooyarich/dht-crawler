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
package org.zxg.network.dhtcrawler.bt;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.zxg.network.dhtcrawler.CrawlerListener;

/**
 *
 * @author Xianguang Zhou <xianguang.zhou@outlook.com>
 */
public class MetaDataDownloadTask implements Runnable {

    private byte[] infoHash;
    private String peerIp;
    private int peerPort;
    private byte[] peerNodeId;
    private CrawlerListener crawlerListener;

    public MetaDataDownloadTask(byte[] infoHash, String peerIp, int peerPort, byte[] peerNodeId, CrawlerListener crawlerListener) {
        this.infoHash = infoHash;
        this.peerIp = peerIp;
        this.peerPort = peerPort;
        this.peerNodeId = peerNodeId;
        this.crawlerListener = crawlerListener;
    }

    @Override
    public void run() {
//        System.out.println("download meta data");// TODO
        try (Socket peerSocket = new Socket(peerIp, peerPort);
                OutputStream output = peerSocket.getOutputStream();
                InputStream input = peerSocket.getInputStream()) {
            byte[] handshakeMsg = new byte[68];
            handshakeMsg[0] = 19;
            System.arraycopy("BitTorrent protocol".getBytes(), 0, handshakeMsg, 1, 19);
            handshakeMsg[25] = 16;
            System.arraycopy(infoHash, 0, handshakeMsg, 28, 20);
            System.arraycopy(peerNodeId, 0, handshakeMsg, 48, 20);
            output.write(handshakeMsg);

            byte[] replyMsg = new byte[65535];
            int totalBytes = 0;
            int n;
            System.out.print("handshake reply:");
            byte[] lengthBytes = new byte[8];
            while ((n = input.read(lengthBytes, totalBytes + 4, 4 - totalBytes)) != -1) {
                totalBytes += n;
                if (totalBytes == 4) {
                    break;
                }
            }
            long length = ByteBuffer.wrap(lengthBytes).getLong();
            System.out.print("length prefix:" + length + ",");
            while ((n = input.read(replyMsg)) != -1) {
                totalBytes += n;
                if (n == 0) {
                    break;
                }
//                System.out.print(new String(replyMsg, 0, n));
            }
            System.out.println("total bytes:" + totalBytes);
        } catch (ConnectException ex) {
            Logger.getLogger(BtCrawler.class.getName()).log(Level.FINE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(BtCrawler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
