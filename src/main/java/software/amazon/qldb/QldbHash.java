/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package software.amazon.qldb;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ionhash.IonHashReader;
import com.amazon.ionhash.IonHashReaderBuilder;
import com.amazon.ionhash.MessageDigestIonHasherProvider;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>A QLDB hash is either a 256 bit number or a special empty hash. See
 * <a href='https://docs.aws.amazon.com/qldb/latest/developerguide/verification.html'>QLDB Verification</a></p>
 *
 * <p>
 *     The QLDB hash is used to create the commit hash of the transaction.
 *     The QLDB hash information is generated using the  transaction id with the statements and their
 *     parameters used in the transaction.
 * </p>
 */
public class QldbHash {
    private static final Logger logger = LoggerFactory.getLogger(QldbHash.class);
    private static final int HASH_SIZE = 32;
    private static final MessageDigestIonHasherProvider HASHER_PROVIDER = new MessageDigestIonHasherProvider("SHA-256");

    private byte[] qldbHash;
    private final IonSystem ionSystem;

    public QldbHash(byte[] qldbHash, IonSystem ionSystem) {
        if (qldbHash == null || !(qldbHash.length == HASH_SIZE || qldbHash.length == 0)) {
            throw new IllegalArgumentException(String.format("Hashes must either be empty or %d bytes long",
                                                             HASH_SIZE));
        }
        this.ionSystem = ionSystem;
        this.qldbHash = qldbHash;
    }

    public static QldbHash toQldbHash(String value, IonSystem ionSystem) {
        if (null == value) {
            return toQldbHash(ionSystem.newString(""), ionSystem);
        }
        return toQldbHash(ionSystem.newString(value), ionSystem);
    }

    /**
     * The QLDB Hash of an IonValue is just the IonHash of that value.
     * @param value
     *          The Ion value to hash.
     * @param ionSystem
     *          The Ion system that is used to read the Ion value.
     * @return An instance of the `QldbHash` with the hashed Ion values.
     */
    public static QldbHash toQldbHash(IonValue value, IonSystem ionSystem) {
        IonReader reader = ionSystem.newReader(value);
        IonHashReader hashReader = IonHashReaderBuilder.standard()
                .withHasherProvider(HASHER_PROVIDER)
                .withReader(reader)
                .build();
        while (hashReader.next() != null) {
        } // read the IonValue
        return new QldbHash(hashReader.digest(), ionSystem);
    }

    /**
     * The QLDB dot operator joins two hashes and generates a new hash.
     * @param that
     *          The Ion value to hash.
     * @return An instance of the `QldbHash` with the hashed ion values.
     */
    public QldbHash dot(QldbHash that) {
        byte[] concatenated = joinHashesPairwise(this.getQldbHash(), that.getQldbHash());
        MessageDigest messageDigest = newMessageDigest();
        messageDigest.update(concatenated);
        return new QldbHash(messageDigest.digest(), ionSystem);
    }

    public boolean equals(Object other) {
        if (null == other || !(other instanceof QldbHash)) {
            return false;
        }
        return hashComparator.compare(this.getQldbHash(), ((QldbHash) other).getQldbHash()) == 0;
    }

    public int getHashSize() {
        return qldbHash.length;
    }

    /**
     * Retrieves the `QldbHash` bytes.
     * @return The `QldbHash` in bytes.
     */
    
    public byte[] getQldbHash() {
        return this.qldbHash;
    }

    public int hashCode() {
        return Arrays.hashCode(getQldbHash());
    }

    public boolean isEmpty() {
        return qldbHash.length == 0;
    }

    public String toString() {
        StringBuffer stringBuffer = new StringBuffer();
        for (byte it : qldbHash) {
            stringBuffer.append(String.format("%02x", it));
        }
        return stringBuffer.toString();
    }

    /**
     * Takes two hashes, sorts them, and concatenates them.
     */
    private static byte[] joinHashesPairwise(byte[] h1, byte[] h2) {
        if (h1.length == 0) {
            return h2;
        }
        if (h2.length == 0) {
            return h1;
        }
        byte[] concatenated = new byte[h1.length + h2.length];
        if (hashComparator.compare(h1, h2) < 0) {
            System.arraycopy(h1, 0, concatenated, 0, h1.length);
            System.arraycopy(h2, 0, concatenated, h1.length, h2.length);
        } else {
            System.arraycopy(h2, 0, concatenated, 0, h2.length);
            System.arraycopy(h1, 0, concatenated, h2.length, h1.length);
        }
        return concatenated;
    }

    /**
     * Get a new instance of {@link MessageDigest} using the SHA-256 algorithm. If this algorithm is
     * not available on the current JVM then throws {@link IllegalStateException}
     */
    private static MessageDigest newMessageDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            logger.error("Failed to create SHA-256 MessageDigest", e);
            throw new IllegalStateException("SHA-256 message digest is unavailable", e);
        }
    }

    /**
     * Compares two hashes by their <em>signed</em> byte values in little-endian order.
     */
    // CHECKSTYLE:OFF: DeclarationOrder - Implement comparator as lambda conflicts with checkstyle
    private static Comparator<byte[]> hashComparator = (h1, h2) -> {
        // CHECKSTYLE:ON: DeclarationOrder
        if (h1.length != 32 || h2.length != 32) {
            throw new IllegalArgumentException("Invalid hash");
        }
        for (int i = h1.length - 1; i >= 0; i--) {
            int byteEqual = Byte.compare(h1[i], h2[i]);
            if (byteEqual != 0) {
                return byteEqual;
            }
        }

        return 0;
    };
}
