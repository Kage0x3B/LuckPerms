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

package me.lucko.luckperms.bukkit.messaging;

import com.esotericsoftware.kryonet.Connection;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import de.syscy.kagecloud.network.CloudConnection;
import de.syscy.kagecloud.network.packet.PluginDataPacket;
import de.syscy.kagecloud.network.packet.RelayPacket;
import de.syscy.kagecloud.spigot.KageCloudSpigot;
import de.syscy.kagecloud.util.ICloudPluginDataListener;
import me.lucko.luckperms.api.messenger.IncomingMessageConsumer;
import me.lucko.luckperms.api.messenger.Messenger;
import me.lucko.luckperms.api.messenger.message.OutgoingMessage;
import me.lucko.luckperms.bukkit.LPBukkitPlugin;
import org.bukkit.Bukkit;

import javax.annotation.Nonnull;

/**
 * An implementation of {@link Messenger} using the plugin messaging channels.
 */
public class KageCloudMessenger implements Messenger, ICloudPluginDataListener {
	private static final String CHANNEL = "lpuc";

	private final LPBukkitPlugin plugin;
	private final IncomingMessageConsumer consumer;

	private KageCloudSpigot cloudPlugin;

	public KageCloudMessenger(LPBukkitPlugin plugin, IncomingMessageConsumer consumer) {
		this.plugin = plugin;
		this.consumer = consumer;
	}

	public void init() {
		cloudPlugin = (KageCloudSpigot) Bukkit.getPluginManager().getPlugin("KageCloudSpigot");
		cloudPlugin.registerPluginDataListener(CHANNEL, this);
	}

	@Override
	public void close() {
		this.plugin.getBootstrap().getServer().getMessenger().unregisterIncomingPluginChannel(this.plugin.getBootstrap(), CHANNEL);
		this.plugin.getBootstrap().getServer().getMessenger().unregisterOutgoingPluginChannel(this.plugin.getBootstrap(), CHANNEL);
	}

	@Override
	public void sendOutgoingMessage(@Nonnull OutgoingMessage outgoingMessage) {
		try {
			PluginDataPacket packet = new PluginDataPacket(CHANNEL);
			ByteArrayDataOutput out = packet.out();
			out.writeUTF(outgoingMessage.asEncodedString());

			cloudPlugin.getClient().sendTCP(RelayPacket.toAllOfType(CloudConnection.Type.SERVER, packet));
		} catch(Exception ex) {
			plugin.getLogger()
				  .warn("An error occurred while sending a plugin message: " + ex.getClass()
																				 .getSimpleName() + " (" + ex.getMessage() + ")");
		}
	}

	@Override
	public void onPluginData(Connection connection, PluginDataPacket pluginDataPacket) {
		try {
			ByteArrayDataInput in = pluginDataPacket.in();
			String msg = in.readUTF();

			this.consumer.consumeIncomingMessageAsString(msg);
		} catch(Exception ex) {
			plugin.getLogger()
				  .warn("An error occurred while receiving a plugin message: " + ex.getClass()
																				   .getSimpleName() + " (" + ex.getMessage() + ")");
		}
	}
}
