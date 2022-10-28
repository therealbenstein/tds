defmodule Tds.Stream do
  @moduledoc """
  Stream struct returned from stream commands.

  All of its fields are private.
  """
  @derive {Inspect, only: []}
  defstruct [:conn, :query, :params, :options]
  @type t :: %Tds.Stream{}
end

defmodule Tds.Cursor do
  @moduledoc false
  defstruct [:portal, :ref, :connection_id, :mode]
  @type t :: %Tds.Cursor{}
end

defmodule Tds.Copy do
  @moduledoc false
  defstruct [:portal, :ref, :connection_id, :query]
  @type t :: %Tds.Copy{}
end

defimpl Enumerable, for: Tds.Stream do
  alias Tds.Query

  def reduce(%Tds.Stream{query: %Query{} = query} = stream, acc, fun) do
    %Tds.Stream{conn: conn, params: params, options: opts} = stream
    stream = %DBConnection.Stream{conn: conn, query: query, params: params, opts: opts}
    DBConnection.reduce(stream, acc, fun)
  end

  def reduce(%Tds.Stream{query: statement} = stream, acc, fun) do
    %Tds.Stream{conn: conn, params: params, options: opts} = stream
    query = %Query{name: "", statement: statement}
    opts = Keyword.put(opts, :function, :prepare_open)
    stream = %DBConnection.PrepareStream{conn: conn, query: query, params: params, opts: opts}
    DBConnection.reduce(stream, acc, fun)
  end

  def member?(_, _) do
    {:error, __MODULE__}
  end

  def count(_) do
    {:error, __MODULE__}
  end

  def slice(_) do
    {:error, __MODULE__}
  end
end

defimpl Collectable, for: Tds.Stream do
  alias Tds.Stream
  alias Tds.Query

  def into(%Stream{conn: %DBConnection{}} = stream) do
    %Stream{conn: conn, query: query, params: params, options: opts} = stream
    opts = Keyword.put(opts, :tds_copy, true)

    case query do
      %Query{} ->
        copy = DBConnection.execute!(conn, query, params, opts)
        {:ok, make_into(conn, stream, copy, opts)}

      query ->
        query = %Query{name: "", statement: query}
        {_, copy} = DBConnection.prepare_execute!(conn, query, params, opts)
        {:ok, make_into(conn, stream, copy, opts)}
    end
  end

  def into(_) do
    raise ArgumentError, "data can only be copied to database inside a transaction"
  end

  defp make_into(conn, stream, %Tds.Copy{ref: ref} = copy, opts) do
    fn
      :ok, {:cont, data} ->
        _ = DBConnection.execute!(conn, copy, {:copy_data, ref, data}, opts)
        :ok

      :ok, close when close in [:done, :halt] ->
        _ = DBConnection.execute!(conn, copy, {:copy_done, ref}, opts)
        stream
    end
  end
end

defimpl DBConnection.Query, for: Tds.Copy do
  alias Tds.Copy
  import Tds.Messages

  def parse(copy, _) do
    raise "can not prepare #{inspect(copy)}"
  end

  def describe(copy, _) do
    raise "can not describe #{inspect(copy)}"
  end

  def encode(%Copy{ref: ref}, {:copy_data, ref, data}, _) do
    try do
      encode_msg(msg_copy_data(data: data), ref)
    rescue
      ArgumentError ->
        reraise ArgumentError,
                [message: "expected iodata to copy to database, got: " <> inspect(data)],
                __STACKTRACE__
    else
      iodata ->
        {:copy_data, iodata}
    end
  end

  def encode(%Copy{ref: ref}, {:copy_done, ref}, _) do
    :copy_done
  end

  def decode(copy, _result, _opts) do
    raise "can not describe #{inspect(copy)}"
  end
end

defimpl String.Chars, for: Tds.Copy do
  def to_string(%Tds.Copy{query: query}) do
    String.Chars.to_string(query)
  end
end