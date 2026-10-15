using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace EasyDapper
{
    internal sealed class ParameterBuilder
    {
        private readonly ConcurrentDictionary<string, object> _parameters = new ConcurrentDictionary<string, object>(StringComparer.Ordinal);
        private int _paramCounter = 0;
        private readonly object _parameterLock = new object();
        private readonly HashSet<string> _reservedNames = new HashSet<string>(StringComparer.Ordinal);
        private readonly ThreadLocal<Checkpoint> _checkpoint = new ThreadLocal<Checkpoint>();
        private readonly Dictionary<string, WriteNode> _writeHeads = new Dictionary<string, WriteNode>(StringComparer.Ordinal);
        private readonly HashSet<string> _abandonedReservations = new HashSet<string>(StringComparer.Ordinal);
        private int _activeCheckpoints;

        public string GetUniqueParameterName()
        {
            lock (_parameterLock)
            {
                string name;
                do { name = "@p" + (++_paramCounter).ToString(System.Globalization.CultureInfo.InvariantCulture); }
                while (_parameters.ContainsKey(name) || !_reservedNames.Add(name));
                _checkpoint.Value?.Reservations.Add(name);
                return name;
            }
        }

        public void AddParameter(string name, object value)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException("name");
            lock (_parameterLock)
            {
                if (_reservedNames.Add(name)) _checkpoint.Value?.Reservations.Add(name);
                if (_activeCheckpoints != 0)
                {
                    WriteNode previous;
                    if (!_writeHeads.TryGetValue(name, out previous))
                    {
                        object original;
                        if (_parameters.TryGetValue(name, out original))
                            previous = new WriteNode { Name = name, Value = original };
                    }
                    var write = new WriteNode { Name = name, Value = value, Previous = previous };
                    _writeHeads[name] = write;
                    var checkpoint = _checkpoint.Value;
                    if (checkpoint != null) checkpoint.Writes.Add(write);
                    else
                    {
                        // A write outside a checkpoint is already committed; no
                        // active ancestor may roll it back to an older value.
                        write.Previous = null;
                        _abandonedReservations.Remove(name);
                    }
                }
                _parameters[name] = value;
            }
        }

        public Dictionary<string, string> MergeParameters(IDictionary<string, object> sourceParameters)
        {
            var renamings = new Dictionary<string, string>();
            if (sourceParameters == null) return renamings;
            foreach (var kv in sourceParameters)
            {
                // Imported names belong to a different builder's namespace. Allocate
                // every imported value here so later predicates cannot overwrite it.
                var newName = GetUniqueParameterName();
                renamings[kv.Key] = newName;
                AddParameter(newName, kv.Value);
            }
            return renamings;
        }

        public IDictionary<string, object> GetParameters()
        {
            lock (_parameterLock) return new Dictionary<string, object>(_parameters, StringComparer.Ordinal);
        }

        internal sealed class Checkpoint
        {
            internal Checkpoint Parent;
            internal readonly List<WriteNode> Writes = new List<WriteNode>();
            internal readonly HashSet<string> Reservations = new HashSet<string>(StringComparer.Ordinal);
        }

        internal sealed class WriteNode
        {
            internal string Name;
            internal object Value;
            internal WriteNode Previous;
            internal bool Undone;
        }

        internal Checkpoint CreateCheckpoint()
        {
            lock (_parameterLock)
            {
                var checkpoint = new Checkpoint
                {
                    Parent = _checkpoint.Value
                };
                _activeCheckpoints++;
                _checkpoint.Value = checkpoint;
                return checkpoint;
            }
        }

        internal void CommitCheckpoint(Checkpoint checkpoint)
        {
            lock (_parameterLock)
            {
                // A committed value is a permanent rollback boundary, even
                // when another checkpoint has already written on top of it.
                foreach (var write in checkpoint.Writes) write.Previous = null;
                CompleteCheckpoint(checkpoint);
            }
        }

        internal void RestoreCheckpoint(Checkpoint checkpoint)
        {
            lock (_parameterLock)
            {
                // Undo only this translation's writes. Clauses committed by a
                // captured getter, including another thread, own separate journals.
                for (int index = checkpoint.Writes.Count - 1; index >= 0; index--)
                {
                    var write = checkpoint.Writes[index];
                    write.Undone = true;
                    WriteNode current;
                    if (!_writeHeads.TryGetValue(write.Name, out current)) continue;
                    while (current != null && current.Undone) current = current.Previous;
                    if (current != null)
                    {
                        _writeHeads[write.Name] = current;
                        _parameters[write.Name] = current.Value;
                    }
                    else
                    {
                        _writeHeads.Remove(write.Name);
                        object removed;
                        _parameters.TryRemove(write.Name, out removed);
                        if (_abandonedReservations.Remove(write.Name)) _reservedNames.Remove(write.Name);
                    }
                }
                foreach (var name in checkpoint.Reservations)
                {
                    if (_parameters.ContainsKey(name)) _abandonedReservations.Add(name);
                    else { _reservedNames.Remove(name); _abandonedReservations.Remove(name); }
                }
                int highest = 0;
                foreach (var name in _reservedNames)
                {
                    int suffix;
                    if (name.StartsWith("@p", StringComparison.Ordinal) && int.TryParse(name.Substring(2), out suffix)) highest = Math.Max(highest, suffix);
                }
                _paramCounter = highest;
                CompleteCheckpoint(checkpoint);
            }
        }

        private void CompleteCheckpoint(Checkpoint checkpoint)
        {
            _checkpoint.Value = checkpoint.Parent;
            if (--_activeCheckpoints != 0) return;
            // Ordinary successful translations keep no historical chains.
            _writeHeads.Clear();
            _abandonedReservations.Clear();
        }

        internal ConcurrentDictionary<string, object> GetInternalParameters() => _parameters;

        public List<string> GetOrderedParameterNames()
        {
            return _parameters.Keys.ToList();
        }
    }
}
