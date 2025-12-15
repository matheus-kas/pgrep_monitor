import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import psycopg2
from psycopg2.extras import RealDictCursor
import csv
import json
from datetime import datetime

class ConexaoPostgreSQL:
    def __init__(self):
        self.conn = None
        self.cursor = None
    
    def conectar(self, host, database, user, password, port=5432):
        try:
            self.conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port
            )
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            return True
        except Exception as e:
            messagebox.showerror("Erro de Conexão", f"Erro ao conectar ao banco: {str(e)}")
            return False
    
    def desconectar(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def testar_conexao(self):
        """Testa se a conexão está ativa"""
        try:
            self.cursor.execute("SELECT 1")
            return True
        except:
            return False

class AplicacaoProdutos:
    def __init__(self, root):
        self.root = root
        self.root.title("Sistema de Consulta de Produtos por Grupo")
        self.root.geometry("1400x900")
        
        # Configurar estilo
        self.configurar_estilo()
        
        self.db = ConexaoPostgreSQL()
        self.grupo_selecionado = None
        self.produtos_data = []
        
        self.criar_interface()
    
    def configurar_estilo(self):
        """Configura o estilo da interface"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Configurar cores e fontes
        style.configure('TLabel', font=('Arial', 10))
        style.configure('TButton', font=('Arial', 10))
        style.configure('TEntry', font=('Arial', 10))
        style.configure('TCombobox', font=('Arial', 10))
    
    def criar_interface(self):
        # Frame principal
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Frame de conexão
        frame_conexao = ttk.LabelFrame(main_frame, text="🔗 Conexão com o Banco de Dados", padding=15)
        frame_conexao.pack(fill="x", pady=(0, 10))
        
        # Grid para campos de conexão
        ttk.Label(frame_conexao, text="Host:").grid(row=0, column=0, sticky="w", padx=(0, 5))
        self.host_entry = ttk.Entry(frame_conexao, width=20)
        self.host_entry.grid(row=0, column=1, padx=5, pady=2)
        self.host_entry.insert(0, "localhost")
        
        ttk.Label(frame_conexao, text="Database:").grid(row=0, column=2, sticky="w", padx=(10, 5))
        self.database_entry = ttk.Entry(frame_conexao, width=20)
        self.database_entry.grid(row=0, column=3, padx=5, pady=2)
        self.database_entry.insert(0, "postgres")
        
        ttk.Label(frame_conexao, text="Usuário:").grid(row=1, column=0, sticky="w", padx=(0, 5))
        self.user_entry = ttk.Entry(frame_conexao, width=20)
        self.user_entry.grid(row=1, column=1, padx=5, pady=2)
        self.user_entry.insert(0, "postgres")
        
        ttk.Label(frame_conexao, text="Senha:").grid(row=1, column=2, sticky="w", padx=(10, 5))
        self.password_entry = ttk.Entry(frame_conexao, width=20, show="*")
        self.password_entry.grid(row=1, column=3, padx=5, pady=2)
        self.password_entry.insert(0, "sua_senha")
        
        ttk.Label(frame_conexao, text="Porta:").grid(row=1, column=4, sticky="w", padx=(10, 5))
        self.port_entry = ttk.Entry(frame_conexao, width=10)
        self.port_entry.grid(row=1, column=5, padx=5, pady=2)
        self.port_entry.insert(0, "5432")
        
        self.btn_conectar = ttk.Button(frame_conexao, text="Conectar ao Banco", command=self.conectar_banco)
        self.btn_conectar.grid(row=0, column=6, rowspan=2, padx=10)
        
        # Frame de seleção do grupo
        frame_grupo = ttk.LabelFrame(main_frame, text="📂 Seleção do Grupo de Produto", padding=15)
        frame_grupo.pack(fill="x", pady=(0, 10))
        
        ttk.Label(frame_grupo, text="Grupo de Produto:").pack(side="left")
        self.grupo_combobox = ttk.Combobox(frame_grupo, width=50, state="readonly")
        self.grupo_combobox.pack(side="left", padx=5)
        self.grupo_combobox.bind('<<ComboboxSelected>>', self.selecionar_grupo)
        
        self.btn_carregar_grupos = ttk.Button(frame_grupo, text="🔄 Carregar Grupos", command=self.carregar_grupos)
        self.btn_carregar_grupos.pack(side="left", padx=5)
        
        self.btn_buscar = ttk.Button(frame_grupo, text="🔍 Buscar Produtos", command=self.buscar_produtos, state="disabled")
        self.btn_buscar.pack(side="left", padx=5)
        
        self.btn_exportar = ttk.Button(frame_grupo, text="📊 Exportar CSV", command=self.exportar_csv, state="disabled")
        self.btn_exportar.pack(side="left", padx=5)
        
        # Frame de status
        self.frame_status = ttk.Frame(main_frame)
        self.frame_status.pack(fill="x", pady=(0, 10))
        
        self.status_label = ttk.Label(self.frame_status, text="Status: Não conectado", foreground="red")
        self.status_label.pack(side="left")
        
        self.contador_label = ttk.Label(self.frame_status, text="", foreground="blue")
        self.contador_label.pack(side="right")
        
        # Frame principal para resultados e detalhes
        frame_principal = ttk.Frame(main_frame)
        frame_principal.pack(fill="both", expand=True)
        
        # Frame de resultados
        frame_resultados = ttk.LabelFrame(frame_principal, text="📋 Lista de Produtos", padding=10)
        frame_resultados.pack(side="left", fill="both", expand=True, padx=(0, 5))
        
        # Treeview para mostrar os produtos
        columns = ('codigo', 'nome', 'codigo_barra', 'preco_unit', 'preco_custo', 'margem_lucro', 'estoque')
        self.tree = ttk.Treeview(frame_resultados, columns=columns, show='headings', height=20)
        
        # Definir cabeçalhos
        self.tree.heading('codigo', text='Código')
        self.tree.heading('nome', text='Nome do Produto')
        self.tree.heading('codigo_barra', text='Código Barras')
        self.tree.heading('preco_unit', text='Preço Venda')
        self.tree.heading('preco_custo', text='Preço Custo')
        self.tree.heading('margem_lucro', text='Margem %')
        self.tree.heading('estoque', text='Controla Estoque')
        
        # Definir largura das colunas
        self.tree.column('codigo', width=80, anchor='center')
        self.tree.column('nome', width=250)
        self.tree.column('codigo_barra', width=120, anchor='center')
        self.tree.column('preco_unit', width=100, anchor='e')
        self.tree.column('preco_custo', width=100, anchor='e')
        self.tree.column('margem_lucro', width=80, anchor='center')
        self.tree.column('estoque', width=120, anchor='center')
        
        # Scrollbars
        scrollbar_v = ttk.Scrollbar(frame_resultados, orient="vertical", command=self.tree.yview)
        scrollbar_h = ttk.Scrollbar(frame_resultados, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=scrollbar_v.set, xscrollcommand=scrollbar_h.set)
        
        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar_v.pack(side="right", fill="y")
        scrollbar_h.pack(side="bottom", fill="x")
        
        # Bind para mostrar detalhes ao clicar
        self.tree.bind('<<TreeviewSelect>>', self.mostrar_detalhes)
        self.tree.bind('<Double-1>', self.mostrar_detalhes_completos)
        
        # Frame de detalhes
        self.frame_detalhes = ttk.LabelFrame(frame_principal, text="📄 Detalhes do Produto", padding=10)
        self.frame_detalhes.pack(side="right", fill="both", expand=True, padx=(5, 0))
        
        # Text area para detalhes
        self.text_detalhes = tk.Text(self.frame_detalhes, height=20, wrap="word", font=('Arial', 10))
        scrollbar_text = ttk.Scrollbar(self.frame_detalhes, orient="vertical", command=self.text_detalhes.yview)
        self.text_detalhes.configure(yscrollcommand=scrollbar_text.set)
        
        self.text_detalhes.pack(side="left", fill="both", expand=True)
        scrollbar_text.pack(side="right", fill="y")
    
    def atualizar_status(self, mensagem, cor="black"):
        """Atualiza o status da aplicação"""
        self.status_label.config(text=f"Status: {mensagem}", foreground=cor)
    
    def atualizar_contador(self, quantidade):
        """Atualiza o contador de produtos"""
        self.contador_label.config(text=f"Produtos encontrados: {quantidade}")
    
    def conectar_banco(self):
        host = self.host_entry.get()
        database = self.database_entry.get()
        user = self.user_entry.get()
        password = self.password_entry.get()
        port = self.port_entry.get()
        
        if not all([host, database, user]):
            messagebox.showwarning("Aviso", "Preencha todos os campos de conexão!")
            return
        
        try:
            if self.db.conectar(host, database, user, password, port):
                self.atualizar_status("Conectado", "green")
                self.btn_conectar.config(text="✅ Conectado")
                self.btn_buscar.config(state="normal")
                self.btn_exportar.config(state="normal")
                self.carregar_grupos()
        except Exception as e:
            self.atualizar_status("Erro na conexão", "red")
            messagebox.showerror("Erro", f"Falha na conexão: {str(e)}")
    
    def carregar_grupos(self):
        if not self.db.conn or not self.db.testar_conexao():
            messagebox.showwarning("Aviso", "Conecte ao banco primeiro!")
            return
        
        try:
            self.atualizar_status("Carregando grupos...", "blue")
            query = "SELECT grid, codigo, nome FROM grupo_produto ORDER BY nome"
            self.db.cursor.execute(query)
            grupos = self.db.cursor.fetchall()
            
            if not grupos:
                messagebox.showinfo("Info", "Nenhum grupo encontrado!")
                return
            
            # Formatar para mostrar no combobox
            grupos_formatados = [f"{g['codigo']} - {g['nome']}" for g in grupos]
            self.grupos_dict = {f"{g['codigo']} - {g['nome']}": g['grid'] for g in grupos}
            
            self.grupo_combobox['values'] = grupos_formatados
            self.atualizar_status(f"Carregados {len(grupos)} grupos", "green")
            
        except Exception as e:
            self.atualizar_status("Erro ao carregar grupos", "red")
            messagebox.showerror("Erro", f"Erro ao carregar grupos: {str(e)}")
    
    def selecionar_grupo(self, event):
        grupo_selecionado = self.grupo_combobox.get()
        if grupo_selecionado in self.grupos_dict:
            self.grupo_selecionado = self.grupos_dict[grupo_selecionado]
            self.atualizar_status(f"Grupo selecionado: {grupo_selecionado}", "blue")
    
    def buscar_produtos(self):
        if not self.grupo_selecionado:
            messagebox.showwarning("Aviso", "Selecione um grupo primeiro!")
            return
        
        try:
            self.atualizar_status("Buscando produtos...", "blue")
            
            # Query simplificada para performance
            query = """
            SELECT
                p.codigo,
                p.codigo_barra,
                p.nome,
                gp.codigo AS grupo_codigo,
                gp.nome AS grupo_nome,
                sg.codigo AS subgrupo_codigo,
                sg.nome AS subgrupo_nome,
                p.unid_med,
                p.preco_unit,
                p.preco_custo,
                p.margem_lucro,
                pf.nome AS fornecedor_nome,
                m.nome AS marca_nome,
                p.data_cad,
                p.codigo_ncm,
                p.cest,
                CASE WHEN p.controlar_estoque THEN 'SIM' ELSE 'NÃO' END AS controla_estoque,
                CASE WHEN p.permite_venda THEN 'SIM' ELSE 'NÃO' END AS permite_venda,
                p.tributacao
            FROM produto p
            LEFT JOIN pessoa pf ON p.fornecedor = pf.grid
            LEFT JOIN grupo_produto gp ON p.grupo = gp.grid
            LEFT JOIN subgrupo_produto sg ON p.subgrupo = sg.grid
            LEFT JOIN marca m ON p.marca = m.grid
            WHERE p.flag = 'A' AND p.grupo = %s
            ORDER BY p.codigo
            """
            
            self.db.cursor.execute(query, (self.grupo_selecionado,))
            produtos = self.db.cursor.fetchall()
            
            # Limpar treeview
            for item in self.tree.get_children():
                self.tree.delete(item)
            
            # Preencher treeview
            for produto in produtos:
                preco_unit = f"R$ {produto['preco_unit']:.2f}" if produto['preco_unit'] else 'R$ 0,00'
                preco_custo = f"R$ {produto['preco_custo']:.2f}" if produto['preco_custo'] else 'R$ 0,00'
                margem = f"{produto['margem_lucro']:.1f}%" if produto['margem_lucro'] else '0%'
                
                self.tree.insert('', 'end', values=(
                    produto['codigo'],
                    produto['nome'],
                    produto['codigo_barra'] or '',
                    preco_unit,
                    preco_custo,
                    margem,
                    produto['controla_estoque']
                ))
            
            self.produtos_data = produtos
            self.atualizar_contador(len(produtos))
            self.atualizar_status(f"Busca concluída - {len(produtos)} produtos", "green")
            
            # Mostrar frame de detalhes se estiver oculto
            if not self.frame_detalhes.winfo_ismapped():
                self.frame_detalhes.pack(side="right", fill="both", expand=True, padx=(5, 0))
            
        except Exception as e:
            self.atualizar_status("Erro na busca", "red")
            messagebox.showerror("Erro", f"Erro ao buscar produtos: {str(e)}")
    
    def mostrar_detalhes(self, event):
        selection = self.tree.selection()
        if not selection:
            return
        
        item = self.tree.item(selection[0])
        codigo = item['values'][0]
        
        # Encontrar produto nos dados
        produto = next((p for p in self.produtos_data if p['codigo'] == codigo), None)
        if not produto:
            return
        
        # Formatar detalhes
        detalhes = f"""=== INFORMAÇÕES BÁSICAS ===
Código: {produto['codigo']}
Nome: {produto['nome']}
Código Barras: {produto['codigo_barra'] or 'N/A'}

=== CLASSIFICAÇÃO ===
Grupo: {produto['grupo_codigo']} - {produto['grupo_nome']}
Subgrupo: {produto['subgrupo_codigo']} - {produto['subgrupo_nome']}
Marca: {produto['marca_nome'] or 'N/A'}

=== PREÇOS ===
Preço Venda: R$ {produto['preco_unit']:.2f if produto['preco_unit'] else 0:.2f}
Preço Custo: R$ {produto['preco_custo']:.2f if produto['preco_custo'] else 0:.2f}
Margem Lucro: {produto['margem_lucro'] or 0:.1f}%

=== TRIBUTAÇÃO ===
NCM: {produto['codigo_ncm'] or 'N/A'}
CEST: {produto['cest'] or 'N/A'}
Tributação: {produto['tributacao'] or 'N/A'}

=== CONTROLES ===
Fornecedor: {produto['fornecedor_nome'] or 'N/A'}
Controla Estoque: {produto['controla_estoque']}
Permite Venda: {produto['permite_venda']}
Data Cadastro: {produto['data_cad'] or 'N/A'}
"""
        
        self.text_detalhes.delete(1.0, tk.END)
        self.text_detalhes.insert(1.0, detalhes)
    
    def mostrar_detalhes_completos(self, event):
        """Mostra detalhes completos em uma nova janela"""
        selection = self.tree.selection()
        if not selection:
            return
        
        item = self.tree.item(selection[0])
        codigo = item['values'][0]
        
        # Buscar dados completos do produto
        try:
            query_completa = """
            SELECT * FROM produto p
            LEFT JOIN grupo_produto gp ON p.grupo = gp.grid
            LEFT JOIN subgrupo_produto sg ON p.subgrupo = sg.grid
            WHERE p.codigo = %s AND p.flag = 'A'
            """
            
            self.db.cursor.execute(query_completa, (codigo,))
            produto_completo = self.db.cursor.fetchone()
            
            if produto_completo:
                self.mostrar_janela_detalhes(produto_completo)
                
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao buscar detalhes: {str(e)}")
    
    def mostrar_janela_detalhes(self, produto):
        """Mostra janela com detalhes completos do produto"""
        janela = tk.Toplevel(self.root)
        janela.title(f"Detalhes Completo - {produto['codigo']} - {produto['nome']}")
        janela.geometry("800x600")
        
        text_area = tk.Text(janela, wrap="word", font=('Arial', 10))
        scrollbar = ttk.Scrollbar(janela, orient="vertical", command=text_area.yview)
        text_area.configure(yscrollcommand=scrollbar.set)
        
        # Formatar todos os dados do produto
        detalhes = "=== DETALHES COMPLETOS DO PRODUTO ===\n\n"
        for key, value in produto.items():
            if value is not None and key != 'grid':  # Excluir coluna grid
                detalhes += f"{key}: {value}\n"
        
        text_area.insert(1.0, detalhes)
        text_area.config(state="disabled")
        
        text_area.pack(side="left", fill="both", expand=True, padx=10, pady=10)
        scrollbar.pack(side="right", fill="y")
    
    def exportar_csv(self):
        if not hasattr(self, 'produtos_data') or not self.produtos_data:
            messagebox.showwarning("Aviso", "Nenhum dado para exportar!")
            return
        
        try:
            # Pedir ao usuário onde salvar o arquivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            grupo_nome = self.grupo_combobox.get().split(' - ')[0] if self.grupo_combobox.get() else "grupo"
            default_filename = f"produtos_{grupo_nome}_{timestamp}.csv"
            
            filename = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                initialfile=default_filename
            )
            
            if not filename:
                return  # Usuário cancelou
            
            # Exportar para CSV
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                if self.produtos_data:
                    # Usar as chaves do primeiro produto como cabeçalho
                    fieldnames = self.produtos_data[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    writer.writeheader()
                    for produto in self.produtos_data:
                        writer.writerow(produto)
            
            messagebox.showinfo("Sucesso", f"Dados exportados para:\n{filename}")
            
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao exportar: {str(e)}")

def main():
    try:
        root = tk.Tk()
        app = AplicacaoProdutos(root)
        root.mainloop()
    except Exception as e:
        print(f"Erro ao iniciar aplicação: {e}")
        messagebox.showerror("Erro", f"Falha ao iniciar aplicação: {e}")

if __name__ == "__main__":
    main()