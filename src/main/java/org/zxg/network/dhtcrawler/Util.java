/*
 * Copyright (C) 2014 Xianguang Zhou <xianguang.zhou@outlook.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.zxg.network.dhtcrawler;

import com.turn.ttorrent.bcodec.BDecoder;
import com.turn.ttorrent.bcodec.BEValue;
import com.turn.ttorrent.bcodec.BEncoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

/**
 *
 * @author Xianguang Zhou <xianguang.zhou@outlook.com>
 */
public class Util {

    private static final Random random = new Random();

    public static byte[] entropy(int bytes) {
        byte[] buffer = new byte[bytes];
        for (int i = 0; i < bytes; i++) {
            buffer[i] = (byte) random.nextInt(256);
        }
        return buffer;
    }

    public static byte[] randomId() throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
        messageDigest.update(entropy(20));
        return messageDigest.digest();
    }

    public static byte[] randomId(BigInteger min, BigInteger max) {
        return new BigDecimal(max.subtract(min)).multiply(new BigDecimal(random.nextFloat())).toBigInteger().add(min).toByteArray();
    }

    public static BigInteger nodeIdToInt(byte[] nodeId) {
        return new BigInteger(1, nodeId);
    }

    public static BEValue bdecode(byte[] data, int offset, int length)
            throws IOException {
        BEValue value;
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(data,
                offset, length)) {
            value = BDecoder.bdecode(inputStream);
        }
        return value;
    }

    public static byte[] bencode(BEValue value) throws IOException {
        byte[] data;
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            BEncoder.bencode(value.getValue(), outputStream);
            data = outputStream.toByteArray();
        }
        return data;
    }
}
