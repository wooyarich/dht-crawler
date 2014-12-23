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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.zxg.network.dhtcrawler.CrawlerListener;

/**
 *
 * @author Xianguang Zhou <xianguang.zhou@outlook.com>
 */
public class BtCrawler {

    private ExecutorService executorService;
    private CrawlerListener crawlerListener;

    public BtCrawler(CrawlerListener crawlerListener) {
        this.crawlerListener = crawlerListener;
    }

    public void start() {
        executorService = Executors.newCachedThreadPool();
    }

    public void stop() {
        executorService.shutdownNow();
    }

    public void downloadMetaData(byte[] infoHash, String peerIp, int peerPort, byte[] peerNodeId) {
        executorService.submit(new MetaDataDownloadTask(infoHash, peerIp, peerPort, peerNodeId, this.crawlerListener));
    }

}
