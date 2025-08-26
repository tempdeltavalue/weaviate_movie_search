# -*- coding: utf-8 -*-
import torch
import torch.nn as nn
from transformers import BertTokenizer

# --- Text Embedding LSTM Model ---
class TextEmbeddingLSTM(nn.Module):
    """
    A two-layer LSTM network for generating text embeddings.
    
    This model is designed to process tokenized text and produce a fixed-size
    embedding vector. It's a placeholder and can be easily replaced by
    a different model (e.g., a pre-trained transformer model) in the future.
    """
    def __init__(self, vocab_size, embedding_dim, hidden_dim, output_dim, num_layers=2):
        """
        Initializes the LSTM model.
        
        Args:
            vocab_size (int): The size of the vocabulary.
            embedding_dim (int): The size of the word embedding vectors.
            hidden_dim (int): The number of features in the hidden state of the LSTM.
            output_dim (int): The size of the final output embedding.
            num_layers (int): The number of LSTM layers.
        """
        super(TextEmbeddingLSTM, self).__init__()
        self.embedding = nn.Embedding(vocab_size, embedding_dim)
        self.lstm = nn.LSTM(embedding_dim, hidden_dim, num_layers, batch_first=True)
        self.linear = nn.Linear(hidden_dim, output_dim)

    def forward(self, x):
        """
        Performs the forward pass of the model.
        
        Args:
            x (torch.Tensor): A tensor of token IDs.
            
        Returns:
            torch.Tensor: The final embedding vector for the input sequence.
        """
        embedded = self.embedding(x)
        lstm_out, _ = self.lstm(embedded)
        # We take the output of the last time step for the sequence
        final_embedding = self.linear(lstm_out[:, -1, :])
        return final_embedding

def create_model_and_tokenizer():
    """
    Initializes and returns a tokenizer and the TextEmbeddingLSTM model.
    This function is a single point of entry for changing the model later.
    """
    # Using a real tokenizer from Hugging Face
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
    
    # Model parameters based on the tokenizer's vocabulary size
    vocab_size = tokenizer.vocab_size
    embedding_dim = 128
    hidden_dim = 256
    output_dim = 64
    num_layers = 2
    
    model = TextEmbeddingLSTM(vocab_size, embedding_dim, hidden_dim, output_dim, num_layers)
    
    return tokenizer, model

def get_text_embedding(text, tokenizer, model):
    """
    Tokenizes text and generates an embedding using the provided model.
    
    Args:
        text (str): The input text to embed.
        tokenizer: The tokenizer object (e.g., from Hugging Face).
        model (nn.Module): The PyTorch model to use for embedding.
        
    Returns:
        torch.Tensor: The generated embedding vector.
    """
    # Tokenize the input text and get a PyTorch tensor
    encoded_input = tokenizer(text, return_tensors='pt', padding=True, truncation=True)
    
    with torch.no_grad():
        embedding = model(encoded_input['input_ids'])
        
    return embedding
