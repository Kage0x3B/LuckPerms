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

import me.lucko.luckperms.api.caching.UserData;
import me.lucko.luckperms.api.context.ContextSet;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A player which holds permission data.
 */
public interface User extends PermissionHolder {

    /**
     * Gets the users unique ID
     *
     * @return the users Mojang assigned unique id
     */
    @Nonnull
    UUID getUuid();

    /**
     * Gets the users username
     *
     * <p>Returns null if no username is associated with this user.</p>
     *
     * @return the users username
     */
    @Nullable
    String getName();

    /**
     * Gets the users current primary group.
     *
     * <p>The result of this method depends on which method is configured for primary group calculation. It may not
     * be the same as any value set through {@link #setPrimaryGroup(String)}.</p>
     *
     * @return the users primary group
     */
    @Nonnull
    String getPrimaryGroup();

    /**
     * Sets a users primary group. This will only take effect if platform is using stored primary groups.
     *
     * @param group the new primary group
     * @return if the change was applied successfully
     * @throws IllegalStateException     if the user is not a member of that group
     * @throws NullPointerException      if the group is null
     */
    DataMutateResult setPrimaryGroup(@Nonnull String group);

    /**
     * Gets the user's {@link UserData} cache.
     *
     * @return the users cached data.
     * @since 3.2
     */
    @Nonnull
    UserData getCachedData();

    /**
     * Refreshes and applies any changes to the cached user data.
     *
     * @return the task future
     * @since 4.0
     */
    @Nonnull
    CompletableFuture<Void> refreshCachedData();

    /**
     * Check to see if the user is a direct member of a group
     *
     * @param group The group to check membership of
     * @return true if the user is a member of the group
     * @throws NullPointerException if the group is null
     * @throws IllegalStateException if the group instance was not obtained from LuckPerms.
     */
    boolean isInGroup(@Nonnull Group group);

    /**
     * Check to see if the user is a direct member of a group in a specific context
     *
     * @param group the group to check membership of
     * @param contextSet the context set to filter by
     * @return true if the user is a member of the group
     * @throws IllegalStateException if the group instance was not obtained from LuckPerms.
     * @since 3.2
     */
    boolean isInGroup(@Nonnull Group group, @Nonnull ContextSet contextSet);

    /**
     * Refresh and re-assign the users permissions.
     *
     * <p>This request is not buffered, and the refresh call will be ran directly. This should be called on an
     * asynchronous thread.</p>
     *
     * @deprecated in favour of {@link #refreshCachedData()}.
     */
    @Deprecated
    void refreshPermissions();

    /**
     * Pre-calculates some values in the user's data cache.
     *
     * <p>Is it <b>not</b> necessary to call this method before
     * using {@link #getCachedData()}.</p>
     *
     * @since 2.17
     * @deprecated because use of this method is no longer necessary.
     */
    @Deprecated
    void setupDataCache();

}
