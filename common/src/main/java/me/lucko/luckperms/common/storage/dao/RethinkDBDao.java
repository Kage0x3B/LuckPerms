package me.lucko.luckperms.common.storage.dao;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
import me.lucko.luckperms.LuckPerms;
import me.lucko.luckperms.api.HeldPermission;
import me.lucko.luckperms.api.LogEntry;
import me.lucko.luckperms.api.Node;
import me.lucko.luckperms.api.PlayerSaveResult;
import me.lucko.luckperms.common.actionlog.ExtendedLogEntry;
import me.lucko.luckperms.common.actionlog.Log;
import me.lucko.luckperms.common.bulkupdate.BulkUpdate;
import me.lucko.luckperms.common.bulkupdate.PreparedStatementBuilder;
import me.lucko.luckperms.common.bulkupdate.comparisons.Constraint;
import me.lucko.luckperms.common.contexts.ContextSetJsonSerializer;
import me.lucko.luckperms.common.managers.group.GroupManager;
import me.lucko.luckperms.common.managers.track.TrackManager;
import me.lucko.luckperms.common.model.*;
import me.lucko.luckperms.common.node.factory.NodeFactory;
import me.lucko.luckperms.common.node.model.NodeDataContainer;
import me.lucko.luckperms.common.node.model.NodeHeldPermission;
import me.lucko.luckperms.common.plugin.LuckPermsPlugin;
import me.lucko.luckperms.common.storage.PlayerSaveResultImpl;
import me.lucko.luckperms.common.storage.StorageCredentials;
import me.lucko.luckperms.common.storage.dao.sql.connection.AbstractConnectionFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.annotation.Nullable;
import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RethinkDBDao extends AbstractDao {
	private static final RethinkDB r = RethinkDB.r;
	private static final Type LIST_STRING_TYPE = new TypeToken<List<String>>() {
	}.getType();

	private final String USER_PERMISSIONS_TABLE;
	private final String GROUP_PERMISSIONS_TABLE;
	private final String PLAYERS_TABLE;
	private final String GROUPS_TABLE;
	private final String ACTIONS_TABLE;
	private final String TRACKS_TABLE;

	private final StorageCredentials configuration;
	private final String prefix;
	private final Gson gson;

	private Connection connection = null;

	public RethinkDBDao(LuckPermsPlugin plugin, StorageCredentials configuration, String prefix) {
		super(plugin, "RethinkDB");

		this.configuration = configuration;
		this.prefix = prefix;
		this.gson = new Gson();

		USER_PERMISSIONS_TABLE = prefix + "user_permissions";
		GROUP_PERMISSIONS_TABLE = prefix + "group_permissions";
		PLAYERS_TABLE = prefix + "players";
		GROUPS_TABLE = prefix + "groups";
		ACTIONS_TABLE = prefix + "actions";
		TRACKS_TABLE = prefix + "tracks";
	}

	@Override
	public void init() {
		try {
			if(getConnection() == null) {
				throw new RuntimeException("Could not connect to RethinkDB database!");
			}

			/*List<String> tableNames = Arrays.asList(USER_PERMISSIONS_TABLE, GROUP_PERMISSIONS_TABLE, PLAYERS_TABLE, GROUPS_TABLE, ACTIONS_TABLE, TRACKS_TABLE);

			r.args(r.array(tableNames))
			 .forEach(tableName -> r.tableList()
									.contains(tableName)
									.do_(tableExists -> r.branch(tableExists, r.tableCreate(tableName), r.hashMap())));

			r.args(r.array(tableNames)).forEach(tableName -> r.table(tableName).wait_()).run(getConnection());

			r.table(USER_PERMISSIONS_TABLE).indexCreate("uuid").run(getConnection());
			r.table(USER_PERMISSIONS_TABLE).indexCreate("permission").run(getConnection());
			r.table(GROUP_PERMISSIONS_TABLE).indexCreate("name").run(getConnection());
			r.table(GROUP_PERMISSIONS_TABLE).indexCreate("permission").run(getConnection());
			r.table(PLAYERS_TABLE).indexCreate("uuid").run(getConnection());
			r.table(PLAYERS_TABLE).indexCreate("username").run(getConnection());
			r.table(GROUPS_TABLE).indexCreate("uuid").run(getConnection());*/
		} catch(ReqlError ex) {
			//Usually if table already exists
			ex.printStackTrace();
		}
	}

	public Connection getConnection() {
		if(connection == null || !connection.isOpen()) {
			connection = createConnection();
		}

		return connection;
	}

	private Connection createConnection() {
		String hostname = configuration.getAddress();
		int port = 28015; //Default port
		String databaseName = configuration.getDatabase();

		if(hostname.contains(":")) {
			String[] hostnameSplit = hostname.split(":", 2);
			hostname = hostnameSplit[0];
			port = Integer.parseInt(hostnameSplit[1]);
		}

		Connection.Builder connectionBuilder = r.connection();
		connectionBuilder.hostname(hostname).port(port).db(databaseName);

		if(configuration.getUsername() != null && !configuration.getUsername().isEmpty()) {
			connectionBuilder.user(configuration.getUsername(), configuration.getPassword());
		}

		return connectionBuilder.connect();
	}

	@Override
	public void shutdown() {
		if(connection != null && connection.isOpen()) {
			connection.close();
		}
	}

	@Override
	public void logAction(LogEntry entry) {
		MapObject insertData = r.hashMap("time", entry.getTimestamp());
		insertData.with("actor_uuid", entry.getActor().toString());
		insertData.with("actor_name", entry.getActorName());
		insertData.with("type", Character.toString(entry.getType().getCode()));
		insertData.with("acted_uuid", entry.getActed().map(UUID::toString).orElse("null"));
		insertData.with("acted_name", entry.getActedName());
		insertData.with("action", entry.getAction());

		r.table(ACTIONS_TABLE).insert(insertData).runNoReply(getConnection());
	}

	@Override
	public Log getLog() {
		final Log.Builder log = Log.builder();
		Cursor<Map<String, Object>> cursor = r.table(ACTIONS_TABLE).run(getConnection());

		for(Map<String, Object> logData : cursor) {
			final String actedUuid = (String) logData.get("acted_uuid");

			ExtendedLogEntry logEntry = ExtendedLogEntry.build()
														.timestamp(((long) logData.get("time")))
														.actor(UUID.fromString((String) logData.get("actor_uuid")))
														.actorName((String) logData.get("actor_name"))
														.type(LogEntry.Type.valueOf(((String) logData.get("type")).toCharArray()[0]))
														.acted(actedUuid.equals("null") ? null : UUID.fromString(actedUuid))
														.actedName((String) logData.get("acted_name"))
														.action((String) logData.get("action"))
														.build();

			log.add(logEntry);
		}

		return log.build();
	}

	@Override
	public void applyBulkUpdate(BulkUpdate bulkUpdate) {
		throw new UnsupportedOperationException("Not implemented for a RethinkDB storage.");
	}

	@Override
	public User loadUser(UUID uuid, String username) {
		User user = this.plugin.getUserManager().getOrMake(UserIdentifier.of(uuid, username));
		user.getIoLock().lock();

		try {
			List<NodeDataContainer> data = new ArrayList<>();
			String primaryGroup = null;
			String userName = null;

			// Collect user permissions
			Cursor<Map<String, Object>> playerDataCursor = r.table(USER_PERMISSIONS_TABLE)
															.filter(r.hashMap("uuid", user.getUuid().toString()))
															.run(getConnection());

			for(Map<String, Object> playerDataMap : playerDataCursor) {
				String permission = (String) playerDataMap.get("permission");
				boolean value = (boolean) playerDataMap.get("value");
				String server = (String) playerDataMap.get("server");
				String world = (String) playerDataMap.get("world");
				long expiry = (long) playerDataMap.get("expiry");
				String contexts = (String) playerDataMap.get("contexts");
				data.add(deserializeNode(permission, value, server, world, expiry, contexts));
			}

			// Collect user meta (username & primary group)
			Cursor<Map<String, Object>> mainDataCursor = r.table(PLAYERS_TABLE)
														  .filter(r.hashMap("uuid", user.getUuid().toString()))
														  .run(getConnection());

			if(mainDataCursor.hasNext()) {
				Map<String, Object> mainDataMap = mainDataCursor.next();
				userName = (String) mainDataMap.get("username");
				primaryGroup = (String) mainDataMap.get("primary_group");
			}

			// update username & primary group
			if(primaryGroup == null) {
				primaryGroup = NodeFactory.DEFAULT_GROUP_NAME;
			}
			user.getPrimaryGroup().setStoredValue(primaryGroup);

			// Update their username to what was in the storage if the one in the local instance is null
			user.setName(userName, true);

			// If the user has any data in storage
			if(!data.isEmpty()) {
				Set<Node> nodes = data.stream().map(NodeDataContainer::toNode).collect(Collectors.toSet());
				user.setNodes(NodeMapType.ENDURING, nodes);

				// Save back to the store if data they were given any defaults or had permissions expire
				if(this.plugin.getUserManager().giveDefaultIfNeeded(user, false) | user.auditTemporaryPermissions()) {
					// This should be fine, as the lock will be acquired by the same thread.
					saveUser(user);
				}

			} else {
				// User has no data in storage.
				if(this.plugin.getUserManager().shouldSave(user)) {
					user.clearNodes();
					user.getPrimaryGroup().setStoredValue(null);
					this.plugin.getUserManager().giveDefaultIfNeeded(user, false);
				}
			}
		} finally {
			user.getIoLock().unlock();
		}
		return user;
	}

	@Override
	public void saveUser(User user) {
		user.getIoLock().lock();
		try {
			// Empty data - just delete from the DB.
			if(!this.plugin.getUserManager().shouldSave(user)) {
				r.table(USER_PERMISSIONS_TABLE).filter(r.hashMap("uuid", user.getUuid().toString())).delete().runNoReply(getConnection());

				r.table(PLAYERS_TABLE)
				 .filter(r.hashMap("uuid", user.getUuid().toString()))
				 .update(r.hashMap("primary_group", NodeFactory.DEFAULT_GROUP_NAME))
				 .runNoReply(getConnection());
				return;
			}

			// Get a snapshot of current data.
			Set<NodeDataContainer> remote = new HashSet<>();
			Cursor<Map<String, Object>> permissionDataCursor = r.table(USER_PERMISSIONS_TABLE)
																.filter(r.hashMap("uuid", user.getUuid().toString()))
																.run(getConnection());
			for(Map<String, Object> permissionData : permissionDataCursor) {
				String permission = (String) permissionData.get("permission");
				boolean value = (boolean) permissionData.get("value");
				String server = (String) permissionData.get("server");
				String world = (String) permissionData.get("world");
				long expiry = (long) permissionData.get("expiry");
				String contexts = (String) permissionData.get("contexts");

				remote.add(deserializeNode(permission, value, server, world, expiry, contexts));
			}

			Set<NodeDataContainer> local = user.enduringData()
											   .immutable()
											   .values()
											   .stream()
											   .map(NodeDataContainer::fromNode)
											   .collect(Collectors.toSet());

			Map.Entry<Set<NodeDataContainer>, Set<NodeDataContainer>> diff = compareSets(local, remote);

			Set<NodeDataContainer> toAdd = diff.getKey();
			Set<NodeDataContainer> toRemove = diff.getValue();

			if(!toRemove.isEmpty()) {
				for(NodeDataContainer nd : toRemove) {
					r.table(USER_PERMISSIONS_TABLE)
					 .filter(r.hashMap("uuid", user.getUuid().toString())
							  .with("permission", nd.getPermission())
							  .with("value", nd.getValue())
							  .with("server", nd.getServer())
							  .with("world", nd.getWorld())
							  .with("expiry", nd.getExpiry())
							  .with("contexts", this.gson.toJson(ContextSetJsonSerializer.serializeContextSet(nd.getContexts()))))
					 .delete()
					 .run(getConnection());
				}
			}

			if(!toAdd.isEmpty()) {
				for(NodeDataContainer nd : toAdd) {
					r.table(USER_PERMISSIONS_TABLE)
					 .insert(r.hashMap("uuid", user.getUuid().toString())
							  .with("permission", nd.getPermission())
							  .with("value", nd.getValue())
							  .with("server", nd.getServer())
							  .with("world", nd.getWorld())
							  .with("expiry", nd.getExpiry())
							  .with("contexts", this.gson.toJson(ContextSetJsonSerializer.serializeContextSet(nd.getContexts()))))
					 .runNoReply(getConnection());
				}
			}

			Cursor<Map<String, Object>> cursor = r.table(PLAYERS_TABLE).filter(r.hashMap("uuid", user.getUuid().toString())).run
					(getConnection());

			boolean hasPrimaryGroupSaved = cursor.hasNext();

			if(hasPrimaryGroupSaved) {
				// update
				String newPrimaryGroup = user.getPrimaryGroup().getStoredValue().orElse(NodeFactory.DEFAULT_GROUP_NAME);

				r.table(PLAYERS_TABLE)
				 .filter(r.hashMap("uuid", user.getUuid().toString()))
				 .update(r.hashMap("primary_group", newPrimaryGroup))
				 .run(getConnection());
			} else {
				// insert
				String newPrimaryGroup = user.getPrimaryGroup().getStoredValue().orElse(NodeFactory.DEFAULT_GROUP_NAME);

				r.table(PLAYERS_TABLE)
				 .insert(r.hashMap("uuid", user.getUuid().toString())
						  .with("username", user.getName().orElse("null"))
						  .with("primary_group", newPrimaryGroup))
				 .run(getConnection());
			}
		} finally {
			user.getIoLock().unlock();
		}
	}

	@Override
	public Set<UUID> getUniqueUsers() {
		Set<UUID> uuids = new HashSet<>();

		List<Map<String, Object>> cursor = r.table(USER_PERMISSIONS_TABLE).pluck("uuid").distinct().run(getConnection());

		for(Map<String, Object> dataMap : cursor) {
			uuids.add(UUID.fromString((String) dataMap.get("uuid")));
		}

		return uuids;
	}

	@Override
	public List<HeldPermission<UUID>> getUsersWithPermission(Constraint constraint) {
		Cursor<Map<String, Object>> foundPermissions = r.table(USER_PERMISSIONS_TABLE)
														.filter(constraint.createReqlFilter("permission"))
														.run(getConnection());

		List<HeldPermission<UUID>> held = new ArrayList<>();

		for(Map<String, Object> permissionData : foundPermissions) {
			UUID holder = UUID.fromString((String) permissionData.get("uuid"));
			String perm = (String) permissionData.get("permission");
			boolean value = (boolean) permissionData.get("value");
			String server = (String) permissionData.get("server");
			String world = (String) permissionData.get("world");
			long expiry = (long) permissionData.get("expiry");
			String contexts = (String) permissionData.get("contexts");

			NodeDataContainer data = deserializeNode(perm, value, server, world, expiry, contexts);
			held.add(NodeHeldPermission.of(holder, data));
		}

		return held;
	}

	@Override
	public Group createAndLoadGroup(String name) {
		r.table(GROUPS_TABLE).insert(r.hashMap("name", name)).optArg("conflict", "update").run(getConnection());

		return loadGroup(name).get();
	}

	@Override
	public Optional<Group> loadGroup(String name) {
		// Check the group actually exists
		boolean groupExists = r.table(GROUPS_TABLE).filter(r.hashMap("name", name)).count().gt(0).run(getConnection());

		if(!groupExists) {
			return Optional.empty();
		}

		Group group = this.plugin.getGroupManager().getOrMake(name);
		group.getIoLock().lock();

		try {
			List<NodeDataContainer> data = new ArrayList<>();

			Cursor<Map<String, Object>> groupDataCursor = r.table(GROUP_PERMISSIONS_TABLE)
														   .filter(r.hashMap("name", group.getName()))
														   .run(getConnection());

			for(Map<String, Object> groupDataMap : groupDataCursor) {
				String permission = (String) groupDataMap.get("permission");
				boolean value = (boolean) groupDataMap.get("value");
				String server = (String) groupDataMap.get("server");
				String world = (String) groupDataMap.get("world");
				long expiry = (long) groupDataMap.get("expiry");
				String contexts = (String) groupDataMap.get("contexts");
				data.add(deserializeNode(permission, value, server, world, expiry, contexts));
			}

			if(!data.isEmpty()) {
				Set<Node> nodes = data.stream().map(NodeDataContainer::toNode).collect(Collectors.toSet());
				group.setNodes(NodeMapType.ENDURING, nodes);
			} else {
				group.clearNodes();
			}
		} finally {
			group.getIoLock().unlock();
		}
		return Optional.of(group);
	}

	@Override
	public void loadAllGroups() {
		List<String> groups = new ArrayList<>();

		Cursor<Map<String, Object>> groupCursor = r.table(GROUPS_TABLE).run(getConnection());

		for(Map<String, Object> groupMap : groupCursor) {
			groups.add(((String) groupMap.get("name")).toLowerCase());
		}

		boolean success = true;
		for(String g : groups) {
			try {
				loadGroup(g);
			} catch(Exception e) {
				e.printStackTrace();
				success = false;
			}
		}

		if(!success) {
			throw new RuntimeException("Exception occurred whilst loading a group");
		}

		GroupManager<?> gm = this.plugin.getGroupManager();
		gm.getAll().values().stream().filter(g -> !groups.contains(g.getName())).forEach(gm::unload);
	}

	@Override
	public void saveGroup(Group group) {
		group.getIoLock().lock();

		try {
			// Empty data, just delete.
			if(group.enduringData().immutable().isEmpty()) {
				r.table(GROUP_PERMISSIONS_TABLE).filter(r.hashMap("name", group.getName())).delete().runNoReply(getConnection());

				return;
			}

			// Get a snapshot of current data
			Cursor<Map<String, Object>> groupDataCursor = r.table(GROUP_PERMISSIONS_TABLE)
														   .filter(r.hashMap("name", group.getName()))
														   .run(getConnection());

			Set<NodeDataContainer> remote = new HashSet<>();
			for(Map<String, Object> groupDataMap : groupDataCursor) {
				String permission = (String) groupDataMap.get("permission");
				boolean value = (boolean) groupDataMap.get("value");
				String server = (String) groupDataMap.get("server");
				String world = (String) groupDataMap.get("world");
				long expiry = (long) groupDataMap.get("expiry");
				String contexts = (String) groupDataMap.get("contexts");

				remote.add(deserializeNode(permission, value, server, world, expiry, contexts));
			}

			Set<NodeDataContainer> local = group.enduringData()
												.immutable()
												.values()
												.stream()
												.map(NodeDataContainer::fromNode)
												.collect(Collectors.toSet());

			Map.Entry<Set<NodeDataContainer>, Set<NodeDataContainer>> diff = compareSets(local, remote);

			Set<NodeDataContainer> toAdd = diff.getKey();
			Set<NodeDataContainer> toRemove = diff.getValue();

			if(!toRemove.isEmpty()) {
				for(NodeDataContainer nd : toRemove) {
					r.table(GROUP_PERMISSIONS_TABLE)
					 .filter(r.hashMap("name", group.getName())
							  .with("permission", nd.getPermission())
							  .with("value", nd.getValue())
							  .with("server", nd.getServer())
							  .with("world", nd.getWorld())
							  .with("expiry", nd.getExpiry())
							  .with("contexts", this.gson.toJson(ContextSetJsonSerializer.serializeContextSet(nd.getContexts()))))
					 .delete()
					 .runNoReply(getConnection());
				}
			}

			if(!toAdd.isEmpty()) {
				for(NodeDataContainer nd : toAdd) {
					r.table(GROUP_PERMISSIONS_TABLE)
					 .insert(r.hashMap("name", group.getName())
							  .with("permission", nd.getPermission())
							  .with("value", nd.getValue())
							  .with("server", nd.getServer())
							  .with("world", nd.getWorld())
							  .with("expiry", nd.getExpiry())
							  .with("contexts", this.gson.toJson(ContextSetJsonSerializer.serializeContextSet(nd.getContexts()))))
					 .runNoReply(getConnection());
				}
			}
		} finally {
			group.getIoLock().unlock();
		}
	}

	@Override
	public void deleteGroup(Group group) {
		group.getIoLock().lock();
		try {
			r.table(GROUP_PERMISSIONS_TABLE).filter(r.hashMap("name", group.getName())).delete().runNoReply(getConnection());
			r.table(GROUPS_TABLE).filter(r.hashMap("name", group.getName())).delete().runNoReply(getConnection());
		} finally {
			group.getIoLock().unlock();
		}

		this.plugin.getGroupManager().unload(group);
	}

	@Override
	public List<HeldPermission<String>> getGroupsWithPermission(Constraint constraint) {
		Cursor<Map<String, Object>> foundPermissions = r.table(GROUP_PERMISSIONS_TABLE)
														.filter(constraint.createReqlFilter("permission"))
														.run(getConnection());
		List<HeldPermission<String>> held = new ArrayList<>();

		for(Map<String, Object> permissionData : foundPermissions) {
			String holder = (String) permissionData.get("name");
			String perm = (String) permissionData.get("permission");
			boolean value = (boolean) permissionData.get("value");
			String server = (String) permissionData.get("server");
			String world = (String) permissionData.get("world");
			long expiry = (long) permissionData.get("expiry");
			String contexts = (String) permissionData.get("contexts");

			NodeDataContainer data = deserializeNode(perm, value, server, world, expiry, contexts);
			held.add(NodeHeldPermission.of(holder, data));
		}

		return held;
	}

	@Override
	public Track createAndLoadTrack(String name) {
		Track track = this.plugin.getTrackManager().getOrMake(name);
		track.getIoLock().lock();
		try {
			boolean exists = false;
			String groups = null;

			Cursor<Map<String, Object>> trackCursor = r.table(TRACKS_TABLE).filter(r.hashMap("name", track.getName())).run(getConnection());

			if(trackCursor.hasNext()) {
				Map<String, Object> existingTrack = trackCursor.next();
				exists = true;
				groups = (String) existingTrack.get("groups");
			}

			if(exists) {
				// Track exists, let's load.
				track.setGroups(this.gson.fromJson(groups, LIST_STRING_TYPE));
			} else {
				String json = this.gson.toJson(track.getGroups());

				r.table(TRACKS_TABLE).insert(r.hashMap("name", track.getName()).with("groups", json)).runNoReply(getConnection());
			}
		} finally {
			track.getIoLock().unlock();
		}

		return track;
	}

	@Override
	public Optional<Track> loadTrack(String name) {
		Track track = this.plugin.getTrackManager().getIfLoaded(name);

		if(track != null) {
			track.getIoLock().lock();
		}

		try {
			String groups;

			Cursor<Map<String, Object>> trackCursor = r.table(TRACKS_TABLE).filter(r.hashMap("name", name)).run(getConnection());

			if(trackCursor.hasNext()) {
				Map<String, Object> existingTrack = trackCursor.next();
				groups = (String) existingTrack.get("groups");
			} else {
				return Optional.empty();
			}

			if(track == null) {
				track = this.plugin.getTrackManager().getOrMake(name);
				track.getIoLock().lock();
			}

			track.setGroups(this.gson.fromJson(groups, LIST_STRING_TYPE));

			return Optional.of(track);

		} finally {
			if(track != null) {
				track.getIoLock().unlock();
			}
		}
	}

	@Override
	public void loadAllTracks() {
		Cursor<Map<String, Object>> trackCursor = r.table(TRACKS_TABLE).run(getConnection());

		List<String> tracks = new ArrayList<>();
		for(Map<String, Object> trackDataMap : trackCursor) {
			tracks.add((String) trackDataMap.get("name"));
		}

		boolean success = true;
		for(String t : tracks) {
			try {
				loadTrack(t);
			} catch(Exception e) {
				e.printStackTrace();
				success = false;
			}
		}

		if(!success) {
			throw new RuntimeException("Exception occurred whilst loading a track");
		}

		TrackManager<?> tm = this.plugin.getTrackManager();
		tm.getAll().values().stream().filter(t -> !tracks.contains(t.getName())).forEach(tm::unload);
	}

	@Override
	public void saveTrack(Track track) {
		track.getIoLock().lock();
		try {
			String groupsJson = this.gson.toJson(track.getGroups());

			r.table(TRACKS_TABLE)
			 .filter(r.hashMap("name", track.getName()))
			 .update(r.hashMap("groups", groupsJson))
			 .runNoReply(getConnection());
		} finally {
			track.getIoLock().unlock();
		}
	}

	@Override
	public void deleteTrack(Track track) {
		track.getIoLock().lock();
		try {
			r.table(TRACKS_TABLE).filter(r.hashMap("name", track.getName())).delete().runNoReply(getConnection());
		} finally {
			track.getIoLock().unlock();
		}

		this.plugin.getTrackManager().unload(track);
	}

	@Override
	public PlayerSaveResult savePlayerData(UUID uuid, String usernameParam) {
		String username = usernameParam.toLowerCase();

		// find any existing mapping
		String oldUsername = getPlayerName(uuid);

		// do the insert
		if(!username.equalsIgnoreCase(oldUsername)) {
			if(oldUsername != null) {
				r.table(PLAYERS_TABLE)
				 .filter(r.hashMap("uuid", uuid.toString()))
				 .update(r.hashMap("username", username))
				 .run(getConnection());
			} else {
				r.table(PLAYERS_TABLE)
				 .insert(r.hashMap("uuid", uuid.toString())
						  .with("username", username)
						  .with("primary_group", NodeFactory.DEFAULT_GROUP_NAME))
				 .run(getConnection());
			}
		}

		PlayerSaveResultImpl result = PlayerSaveResultImpl.determineBaseResult(username, oldUsername);

		Set<UUID> conflicting = new HashSet<>();
		Cursor<Map<String, Object>> conflictingEntriesCursor = r.table(PLAYERS_TABLE)
																.filter(player -> player.g("username")
																						.eq(username)
																						.and(r.not(player.g("uuid").eq(uuid.toString()))))
																.run(getConnection());

		for(Map<String, Object> conflictingEntry : conflictingEntriesCursor) {
			conflicting.add(UUID.fromString((String) conflictingEntry.get("uuid")));
		}

		if(!conflicting.isEmpty()) {
			// remove the mappings for conflicting uuids
			r.table(PLAYERS_TABLE)
			 .filter(player -> player.g("username").eq(username).and(r.not(player.g("uuid").eq(uuid.toString()))))
			 .delete()
			 .runNoReply(getConnection());
			result = result.withOtherUuidsPresent(conflicting);
		}

		return result;
	}

	@Nullable
	@Override
	public UUID getPlayerUuid(String username) {
		Cursor<Map<String, Object>> cursor = r.table(PLAYERS_TABLE)
											  .filter(r.hashMap("username", username))
											  .pluck("uuid")
											  .limit(1)
											  .run(getConnection());

		return cursor.hasNext() ? UUID.fromString((String) cursor.next().get("uuid")) : null;
	}

	@Nullable
	@Override
	public String getPlayerName(UUID uuid) {
		Cursor<Map<String, Object>> cursor = r.table(PLAYERS_TABLE)
											  .filter(r.hashMap("uuid", uuid.toString()))
											  .pluck("username")
											  .limit(1)
											  .run(getConnection());

		return cursor.hasNext() ? (String) cursor.next().get("username") : null;
	}

	/**
	 * Compares two sets
	 * @param local the local set
	 * @param remote the remote set
	 * @return the entries to add to remote, and the entries to remove from remote
	 */
	private static Map.Entry<Set<NodeDataContainer>, Set<NodeDataContainer>> compareSets(Set<NodeDataContainer> local, Set<NodeDataContainer> remote) {
		// entries in local but not remote need to be added
		// entries in remote but not local need to be removed

		Set<NodeDataContainer> toAdd = new HashSet<>(local);
		toAdd.removeAll(remote);

		Set<NodeDataContainer> toRemove = new HashSet<>(remote);
		toRemove.removeAll(local);

		return Maps.immutableEntry(toAdd, toRemove);
	}

	private NodeDataContainer deserializeNode(String permission, boolean value, String server, String world, long expiry, String contexts) {
		return NodeDataContainer.of(permission, value, server, world, expiry, ContextSetJsonSerializer.deserializeContextSet(this.gson, contexts)
																									  .makeImmutable());
	}
}
