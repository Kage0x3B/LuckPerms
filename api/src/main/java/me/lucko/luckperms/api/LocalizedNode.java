/*
 * This file is part of LuckPerms, licensed under the MIT License.
 *
 *  Copyright (c) lucko (Luck) <luck@lucko.me>
 *  Copyright (c) contributors
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package me.lucko.luckperms.api;

import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * A node with a traceable origin
 *
 * @since 2.11
 */
@Immutable
public interface LocalizedNode extends Node {

    /**
     * Returns a predicate which unwraps the localised node parameter before delegating
     * the handling to the provided predicate.
     *
     * @param delegate the delegate predicate.
     * @return the composed predicate
     * @since 4.3
     */
    static Predicate<? super LocalizedNode> composedPredicate(Predicate<Node> delegate) {
        return localizedNode -> delegate.test(localizedNode.getNode());
    }

    /**
     * Gets the delegate node
     *
     * @return the node this instance is representing
     */
    @Nonnull
    Node getNode();

    /**
     * Gets the location where the {@link Node} is inherited from
     *
     * @return where the node was inherited from. Will not return null.
     * @see PermissionHolder#getObjectName()
     */
    @Nonnull
    String getLocation();

}
